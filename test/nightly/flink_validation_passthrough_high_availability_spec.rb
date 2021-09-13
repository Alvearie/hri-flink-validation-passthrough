# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

require_relative '../env'

# Prior to running this test:
#   - run `./gradlew build publishToMavenLocal` in the project's root directory to build the hri-flink-validation-passthrough jar
#   - run `./gradlew copyNightlyTestDependencies` in the project's root directory to prep the jar for this test's use
#
# Anytime the hri-flink-validation-passthrough jar is recompiled, the steps above need to be executed for these tests to pick up the
# new version

describe 'Flink Validation Passthrough High Availability Job' do

  before(:all) do
    TENANT_ID = 'test'
    BATCH_COMPLETION_DELAY = 5000
    @travis_branch = ENV['TRAVIS_BRANCH']
    @flink_helper = FlinkHelper.new(ENV['FLINK_URL'])
    @event_streams_helper = EventStreamsHelper.new
    @flink_api_oauth_token = AppIDHelper.new.get_access_token(Base64.encode64("#{ENV['OIDC_HRI_DATA_INTEGRATOR_CLIENT_ID']}:#{ENV['OIDC_HRI_DATA_INTEGRATOR_CLIENT_SECRET']}").delete("\n"), '', ENV['APPID_FLINK_AUDIENCE'])
    @hri_oauth_token = AppIDHelper.new.get_access_token(Base64.encode64("#{ENV['OIDC_HRI_DATA_INTEGRATOR_CLIENT_ID']}:#{ENV['OIDC_HRI_DATA_INTEGRATOR_CLIENT_SECRET']}").delete("\n"), 'tenant_test hri_data_integrator', ENV['APPID_HRI_AUDIENCE'])
    @hri_helper = HRIHelper.new(ENV['HRI_URL'])
    @elastic = ElasticHelper.new
    @record_validator = KafkaRecordValidator.new
    @helper = Helper.new

    @input_topic = ENV['INPUT_TOPIC'].gsub('.in', "-#{@travis_branch}.in")
    @output_topic = ENV['OUTPUT_TOPIC'].gsub('.out', "-#{@travis_branch}.out")
    @notification_topic = ENV['NOTIFICATION_TOPIC'].gsub('.notification', "-#{@travis_branch}.notification")
    @invalid_topic = ENV['INVALID_TOPIC'].gsub('.invalid', "-#{@travis_branch}.invalid")
    @event_streams_helper.create_topic(@input_topic, 1)
    @event_streams_helper.create_topic(@output_topic, 1)
    @event_streams_helper.create_topic(@notification_topic, 1)
    @event_streams_helper.create_topic(@invalid_topic, 1)
    @event_streams_helper.verify_topic_creation([@input_topic, @output_topic, @notification_topic, @invalid_topic])

    @kafka_notification_builder = KafkaNotificationBuilder.new
    @output_consumer_group = "hri-flink-validation-passthrough-#{@travis_branch}-output-consumer"
    @notification_consumer_group = "hri-flink-validation-passthrough-#{@travis_branch}-notification-consumer"
    @invalid_consumer_group = "hri-flink-validation-passthrough-#{@travis_branch}-invalid-consumer"
    @kafka = Kafka.new(ENV['KAFKA_BROKERS'], client_id: "hri-flink-validation-passthrough-#{@travis_branch}", connect_timeout: 10, socket_timeout: 10, sasl_plain_username: 'token', sasl_plain_password: ENV['SASL_PLAIN_PASSWORD'], ssl_ca_certs_from_system: true)
    @kafka_producer = @kafka.producer(compression_codec: :zstd)

    #Upload Jar File
    @test_jar_id = @flink_helper.upload_jar_from_dir('hri-flink-validation-passthrough-nightly-test-jar.jar', '../dependencies', @flink_api_oauth_token)

    #Start Job
    @test_job_id = @flink_helper.start_flink_job(@test_jar_id, @input_topic, BATCH_COMPLETION_DELAY, @flink_api_oauth_token)
    @flink_helper.verify_job_state(@test_job_id, @flink_api_oauth_token, 'RUNNING')
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
      if @test_job_id
        #Stop Job
        response = @flink_helper.stop_job(@test_job_id, {'Authorization' => "Bearer #{@flink_api_oauth_token}"})
        raise "Failed to stop Flink job with ID: #{@test_job_id}" unless response.code == 202
        @flink_helper.verify_job_state(@test_job_id, @flink_api_oauth_token, 'FINISHED')
      end

      if @test_jar_id
        #Delete Jar
        response = @flink_helper.delete_jar(@test_jar_id, {'Authorization' => "Bearer #{@flink_api_oauth_token}"})
        raise "Failed to delete Flink jar with ID: #{@test_jar_id}" unless response.code == 200
        @flink_helper.verify_jar_deleted(@test_jar_id, @flink_api_oauth_token)
      end

      response = @elastic.es_delete_by_query(TENANT_ID, "name:hri-flink-validation-passthrough-#{ENV['TRAVIS_BRANCH']}*")
      response.nil? ? (raise 'Elastic batch delete did not return a response') : (raise 'Failed to delete Elastic batches' unless response.code == 200)
      Logger.new(STDOUT).info("Delete test batches by query response #{response.body}")

      @kafka_producer.shutdown
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
        name: "hri-flink-validation-passthrough-#{ENV['TRAVIS_BRANCH']}-valid-batch-name",
        dataType: 'hri-flink-validation-passthrough-batch',
        topic: @input_topic
    }
    @batch_id = @hri_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
      @kafka_producer.deliver_messages
      if key == 10
        taskmanager_pod = @helper.exec_command("kubectl get pods -n #{ENV['NAMESPACE']}").split("\n").select { |s| s.include?('taskmanager') }[0].split(' ')[0]
        @helper.exec_command("kubectl delete pod #{taskmanager_pod} -n #{ENV['NAMESPACE']}")
        raise "Kubernetes pod #{taskmanager_pod} not deleted" unless @helper.exec_command("kubectl get pods -n #{ENV['NAMESPACE']}").split("\n").select { |s| s.include?(taskmanager_pod) }.empty?
        Logger.new(STDOUT).info("Deleted taskmanager pod #{taskmanager_pod}")
      end
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, key - 1, true, 120)

    response = @hri_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted completed))
  end

  it 'should persist jars and output all successfully validated records with the same key, headers, and body after deleting the flink jobmanager pod' do
    expected_record_count = {
        expectedRecordCount: 15
    }
    batch_template = {
        name: "hri-flink-validation-passthrough-#{ENV['TRAVIS_BRANCH']}-valid-batch-name",
        dataType: 'hri-flink-validation-passthrough-batch',
        topic: @input_topic
    }
    @batch_id = @hri_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
      @kafka_producer.deliver_messages
      if key == 10
        jobmanager_pod = @helper.exec_command("kubectl get pods -n #{ENV['NAMESPACE']}").split("\n").select { |s| s.include?('jobmanager') }[0].split(' ')[0]
        @helper.exec_command("kubectl delete pod #{jobmanager_pod} -n #{ENV['NAMESPACE']}")
        Logger.new(STDOUT).info("Deleted jobmanager pod: #{jobmanager_pod}")
      end
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    jar_found = false
    Timeout.timeout(60, nil, 'Flink did not return a list of jars after 60 seconds') do
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

    response = @hri_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted completed))
  end

  it 'should output all successfully validated records with the same key, headers, and body after deleting a zookeeper pod' do
    expected_record_count = {
      expectedRecordCount: 15
    }
    batch_template = {
      name: "hri-flink-validation-passthrough-#{ENV['TRAVIS_BRANCH']}-valid-batch-name",
      dataType: 'hri-flink-validation-passthrough-batch',
      topic: @input_topic
    }
    @batch_id = @hri_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
      @kafka_producer.deliver_messages
      if key == 10
        zookeeper_pod = @helper.exec_command("kubectl get pods -n #{ENV['NAMESPACE']}").split("\n").select { |s| s.include?('hri-zookeeper') }[0].split(' ')[0]
        @helper.exec_command("kubectl delete pod #{zookeeper_pod} -n #{ENV['NAMESPACE']}")
        Logger.new(STDOUT).info("Deleted zookeeper pod: #{zookeeper_pod}")
        Timeout.timeout(15, nil, 'Zookeeper pod not reinitializing after 15 seconds') do
          while true
            break unless @helper.exec_command("kubectl get pods -n #{ENV['NAMESPACE']}").split("\n").select { |s| s.include?(zookeeper_pod) && s.include?('Init') }.empty?
          end
        end
      end
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, key - 1, true, 120)

    response = @hri_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted completed))
  end

end