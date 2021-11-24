# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

require_relative '../env'

describe 'Flink Validation Passthrough High Availability Job' do

  before(:all) do
    TENANT_ID = 'test'
    BATCH_COMPLETION_DELAY = 5000
    @git_branch = ENV['BRANCH_NAME']
    @namespace = ENV['NAMESPACE']
    @flink_helper = HRITestHelpers::FlinkHelper.new(ENV['FLINK_URL'])
    @event_streams_helper = HRITestHelpers::EventStreamsHelper.new
    @iam_token = HRITestHelpers::IAMHelper.new(ENV['IAM_CLOUD_URL']).get_access_token(ENV['CLOUD_API_KEY'])
    @appid_helper = HRITestHelpers::AppIDHelper.new(ENV['APPID_URL'], ENV['APPID_TENANT'], @iam_token, nil)
    @flink_api_oauth_token = @appid_helper.get_access_token('hri_integration_tenant_test_data_integrator', '', ENV['APPID_FLINK_AUDIENCE'])
    @hri_oauth_token = @appid_helper.get_access_token('hri_integration_tenant_test_data_integrator', 'tenant_test hri_data_integrator', ENV['APPID_HRI_AUDIENCE'])
    @mgmt_api_helper = HRITestHelpers::MgmtAPIHelper.new(ENV['HRI_INGRESS_URL'], @iam_token)
    @elastic = HRITestHelpers::ElasticHelper.new({url: ENV['ELASTIC_URL'], username: ENV['ELASTIC_USER'], password: ENV['ELASTIC_PASSWORD']})
    @record_validator = KafkaRecordValidator.new
    @request_helper = HRITestHelpers::RequestHelper.new

    timestamp = Time.now.to_i
    @input_topic = ENV['INPUT_TOPIC'].gsub('.in', "-#{@git_branch}-#{timestamp}.in")
    @output_topic = ENV['OUTPUT_TOPIC'].gsub('.out', "-#{@git_branch}-#{timestamp}.out")
    @notification_topic = ENV['NOTIFICATION_TOPIC'].gsub('.notification', "-#{@git_branch}-#{timestamp}.notification")
    @invalid_topic = ENV['INVALID_TOPIC'].gsub('.invalid', "-#{@git_branch}-#{timestamp}.invalid")
    @event_streams_helper.create_topic(@input_topic, 1)
    @event_streams_helper.create_topic(@output_topic, 1)
    @event_streams_helper.create_topic(@notification_topic, 1)
    @event_streams_helper.create_topic(@invalid_topic, 1)
    @event_streams_helper.verify_topic_creation([@input_topic, @output_topic, @notification_topic, @invalid_topic])

    @kafka_notification_builder = KafkaNotificationBuilder.new
    @output_consumer_group = "hri-flink-validation-passthrough-#{@git_branch}-#{timestamp}-output-consumer"
    @notification_consumer_group = "hri-flink-validation-passthrough-#{@git_branch}-#{timestamp}-notification-consumer"
    @invalid_consumer_group = "hri-flink-validation-passthrough-#{@git_branch}-#{timestamp}-invalid-consumer"
    @kafka = Kafka.new(ENV['KAFKA_BROKERS'], client_id: "hri-flink-validation-passthrough-#{@git_branch}-#{timestamp}", connect_timeout: 10, socket_timeout: 10, sasl_plain_username: 'token', sasl_plain_password: ENV['SASL_PLAIN_PASSWORD'], ssl_ca_certs_from_system: true)

    #Upload Jar File
    @test_jar_id = @flink_helper.upload_jar_from_dir("hri-flink-validation-passthrough-#{@git_branch}.jar", File.join(File.dirname(__FILE__), '../../build/libs/'), @flink_api_oauth_token, /hri-flink-validation-passthrough-.+.jar/)

    #Start Job
    @flink_job = FlinkJob.new(@flink_helper, @event_streams_helper, @kafka, @test_jar_id, TENANT_ID)
    @flink_job.start_job(@flink_api_oauth_token, BATCH_COMPLETION_DELAY, @input_topic)
  end

  before(:each) do
    #Reset each consumer group to the latest offset
    @kafka_output_consumer = @kafka.consumer(group_id: @output_consumer_group)
    @kafka_output_consumer.subscribe(@output_topic)
    @kafka_notification_consumer = @kafka.consumer(group_id: @notification_consumer_group)
    @kafka_notification_consumer.subscribe(@notification_topic)
    @kafka_invalid_consumer = @kafka.consumer(group_id: @invalid_consumer_group)
    @kafka_invalid_consumer.subscribe(@invalid_topic)

    consumer_groups = @event_streams_helper.get_groups
    @event_streams_helper.reset_consumer_group(consumer_groups, @output_consumer_group, @output_topic, 'latest')
    @event_streams_helper.reset_consumer_group(consumer_groups, @notification_consumer_group, @notification_topic, 'latest')
    @event_streams_helper.reset_consumer_group(consumer_groups, @invalid_consumer_group, @invalid_topic, 'latest')
    @event_streams_helper.reset_consumer_group(consumer_groups, "hri-validation-#{@input_topic}-#{@output_topic}", @input_topic, 'latest')
    @event_streams_helper.reset_consumer_group(consumer_groups, "hri-validation-#{@input_topic}-#{@output_topic}", @notification_topic, 'latest')
    @event_streams_helper.reset_consumer_group(consumer_groups, "hri-validation-#{@input_topic}-#{@output_topic}", @invalid_topic, 'latest')
  end

  after(:each) do
    @kafka_output_consumer.stop
    @kafka_notification_consumer.stop
    @kafka_invalid_consumer.stop
  end

  after(:all) do
    begin
      #Stop Job
      begin
        @flink_job.cleanup_job(@flink_api_oauth_token)
      rescue Exception => e
        Logger.new(STDOUT).error(e)
      end

      if @test_jar_id
        Logger.new(STDOUT).info('Deleting Validation Jar')
        response = @flink_helper.delete_jar(@test_jar_id, {'Authorization' => "Bearer #{@flink_api_oauth_token}"})
        raise "Failed to delete Flink jar with ID: #{@test_jar_id}" unless response.code == 200
        @flink_helper.verify_jar_deleted(@test_jar_id, @flink_api_oauth_token)
      end

      response = @elastic.es_delete_by_query(TENANT_ID, "name:hri-flink-validation-passthrough-#{@git_branch}*")
      response.nil? ? (raise 'Elastic batch delete did not return a response') : (raise 'Failed to delete Elastic batches' unless response.code == 200)
      Logger.new(STDOUT).info("Delete test batches by query response #{response.body}")
    ensure
      @event_streams_helper.delete_topic(@input_topic)
      @event_streams_helper.delete_topic(@output_topic)
      @event_streams_helper.delete_topic(@notification_topic)
      @event_streams_helper.delete_topic(@invalid_topic)
    end
  end

  it 'should output all successfully validated records with the same key, headers, and body after deleting a flink taskmanager pod' do
    expected_record_count = {
        expectedRecordCount: 15
    }
    batch_template = {
        name: "hri-flink-validation-passthrough-#{@git_branch}-valid-batch-name",
        dataType: 'hri-flink-validation-passthrough-batch',
        topic: @input_topic
    }
    @batch_id = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
      @flink_job.kafka_producer.deliver_messages
      if key == 10
        puts "KUBECTL OUTPUT: #{@request_helper.exec_command("kubectl get namespaces")[:stdout]}"
        taskmanager_pod = @request_helper.exec_command("kubectl get pods -n #{@namespace}")[:stdout].split("\n").select { |s| s.include?('taskmanager') }[0].split(' ')[0]
        @request_helper.exec_command("kubectl delete pod #{taskmanager_pod} -n #{@namespace}")
        raise "Kubernetes pod #{taskmanager_pod} not deleted" unless @request_helper.exec_command("kubectl get pods -n #{@namespace}")[:stdout].split("\n").select { |s| s.include?(taskmanager_pod) }.empty?
        Logger.new(STDOUT).info("Deleted taskmanager pod #{taskmanager_pod}")
      end
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, key - 1, true, 120)

    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted completed))
  end

  it 'should persist jars and output all successfully validated records with the same key, headers, and body after deleting the flink jobmanager pod' do
    expected_record_count = {
        expectedRecordCount: 15
    }
    batch_template = {
        name: "hri-flink-validation-passthrough-#{@git_branch}-valid-batch-name",
        dataType: 'hri-flink-validation-passthrough-batch',
        topic: @input_topic
    }
    @batch_id = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
      @flink_job.kafka_producer.deliver_messages
      if key == 10
        jobmanager_pod = @request_helper.exec_command("kubectl get pods -n #{@namespace}")[:stdout].split("\n").select { |s| s.include?('jobmanager') }[0].split(' ')[0]
        @request_helper.exec_command("kubectl delete pod #{jobmanager_pod} -n #{@namespace}")
        Logger.new(STDOUT).info("Deleted jobmanager pod: #{jobmanager_pod}")
      end
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    jar_found = false
    Timeout.timeout(90, nil, 'Flink did not return a list of jars after 60 seconds') do
      while true
        @response = @flink_helper.get_jars({'Authorization' => "Bearer #{@flink_api_oauth_token}"})
        break if @response.code == 200
      end
    end
    parsed_response = JSON.parse(@response.body)
    parsed_response['files'].each do |file|
      if file['id'] == @test_jar_id
        jar_found = true
        break
      end
    end
    raise "Jar with name 'hri-flink-validation-passthrough-nightly-test-jar.jar' not found" unless jar_found

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, key - 1, true, 120)

    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted completed))
  end

end