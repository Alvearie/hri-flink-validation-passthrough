# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

require_relative '../env'

describe 'Flink Validation Passthrough Job' do

  before(:all) do
    TENANT_ID = 'test'
    BATCH_COMPLETION_DELAY = 5000
    @git_branch = ENV['BRANCH_NAME']
    @flink_helper = HRITestHelpers::FlinkHelper.new(ENV['FLINK_URL'])
    @event_streams_helper = HRITestHelpers::EventStreamsHelper.new
    @iam_token = HRITestHelpers::IAMHelper.new(ENV['IAM_CLOUD_URL']).get_access_token(ENV['CLOUD_API_KEY'])
    @appid_helper = HRITestHelpers::AppIDHelper.new(ENV['APPID_URL'], ENV['APPID_TENANT'], @iam_token, nil)
    @flink_api_oauth_token = @appid_helper.get_access_token('hri_integration_tenant_test_data_integrator', '', ENV['APPID_FLINK_AUDIENCE'])
    @hri_oauth_token = @appid_helper.get_access_token('hri_integration_tenant_test_data_integrator', 'tenant_test hri_data_integrator', ENV['APPID_HRI_AUDIENCE'])
    @mgmt_api_helper = HRITestHelpers::MgmtAPIHelper.new(ENV['HRI_INGRESS_URL'], @iam_token)
    @elastic = HRITestHelpers::ElasticHelper.new({url: ENV['ELASTIC_URL'], username: ENV['ELASTIC_USER'], password: ENV['ELASTIC_PASSWORD']})
    @record_validator = KafkaRecordValidator.new

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
    @kafka_output_consumer = @kafka.consumer(group_id: @output_consumer_group, offset_commit_threshold: 1)
    @kafka_output_consumer.subscribe(@output_topic)
    @kafka_notification_consumer = @kafka.consumer(group_id: @notification_consumer_group, offset_commit_threshold: 1)
    @kafka_notification_consumer.subscribe(@notification_topic)
    @kafka_invalid_consumer = @kafka.consumer(group_id: @invalid_consumer_group, offset_commit_threshold: 1)
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

  it 'should output all records with the same key, headers, and body without validation' do
    batch_info = {
      batch_name: "hri-flink-validation-passthrough-#{@git_branch}-valid-batch-БВГДЖЗИЙЛ",
      batch_data_type: 'hri-flink-validation-passthrough-batch-あいうえおか',
      batch_metadata: 'ᚠᛇᚻ᛫ᛒᛦᚦ᛫ᚠᚱᚩᚠᚢᚱ'
    }
    expected_record_count = {
        expectedRecordCount: 15
    }
    batch_template = {
      name: batch_info[:batch_name],
      dataType: batch_info[:batch_data_type],
      topic: @input_topic,
      metadata: {
        test: batch_info[:batch_metadata]
      }
    }
    @batch_id = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id, testUtf8あいうえおか: 'あいうえおか'})
      @flink_job.kafka_producer.deliver_messages
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, key - 1)

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted completed), BATCH_COMPLETION_DELAY, batch_info.merge(expected_record_count))
  end

  it 'should output all records with the correct batch ID when multiple batches are started' do
    expected_record_count = {
        expectedRecordCount: 15
    }
    batch_template = {
        name: "hri-flink-validation-passthrough-#{@git_branch}-valid-batch-name",
        dataType: 'hri-flink-validation-passthrough-batch',
        topic: @input_topic
    }
    @batch_id_1 = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)
    @batch_id_2 = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key_1 = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key_1}", topic: @input_topic, headers: {batchId: @batch_id_1})
      @flink_job.kafka_producer.deliver_messages
      key_1 += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic for batch 1")
    key_2 = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key_2}", topic: @input_topic, headers: {batchId: @batch_id_2})
      @flink_job.kafka_producer.deliver_messages
      key_2 += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic for batch 2")

    counter = 1
    begin
      Timeout.timeout(60) do
        @kafka_output_consumer.each_message do |message|
          Logger.new(STDOUT).info("Message #{counter} of #{key_1 + key_2 - 2} received")

          if counter < key_1
            @record_validator.single_output_record(message, counter, @batch_id_1)
          else
            @record_validator.single_output_record(message, counter - 15, @batch_id_2)
          end

          counter += 1
          break if counter > (key_1 + key_2 - 2)
        end
      end
      Logger.new(STDOUT).info("All test messages received from the #{@output_topic} topic")
    rescue Timeout::Error
      raise "Timed out waiting for messages from the #{@output_topic} topic. Expected #{key_1 + key_2 - 2} messages, but only received #{counter - 1}."
    end

    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id_1, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id_1} to sendCompleted" unless response.code == 200
    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id_2, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id_2} to sendCompleted" unless response.code == 200

    counter = 1
    records = Array.new(6, false)
    begin
      Timeout.timeout(60) do
        @kafka_notification_consumer.each_message do |message|
          #Verify that all of the correct notification messages are received
          parsed_data = JSON.parse(message.value)
          case [parsed_data['id'], parsed_data['status']]
            when [@batch_id_1, 'started']
              records[0] = true
            when [@batch_id_1, 'sendCompleted']
              @send_completed_time_1 = message.create_time.to_time.to_i
              records[1] = true
            when [@batch_id_1, 'completed']
              @completed_time_1 = message.create_time.to_time.to_i
              raise "Batch completion delay is #{BATCH_COMPLETION_DELAY}ms, but the delay between sendCompleted and completed is only #{(@completed_time_1 - @send_completed_time_1) * 1000}ms" unless (@completed_time_1 - @send_completed_time_1) >= (BATCH_COMPLETION_DELAY / 1000)
              records[2] = true
            when [@batch_id_2, 'started']
              records[3] = true
            when [@batch_id_2, 'sendCompleted']
              @send_completed_time_2 = message.create_time.to_time.to_i
              records[4] = true
            when [@batch_id_2, 'completed']
              @completed_time_2 = message.create_time.to_time.to_i
              raise "Batch completion delay is #{BATCH_COMPLETION_DELAY}ms, but the delay between sendCompleted and completed is only #{(@completed_time_2 - @send_completed_time_2) * 1000}ms" unless (@completed_time_2 - @send_completed_time_2) >= (BATCH_COMPLETION_DELAY / 1000)
              records[5] = true
            else
              raise 'Received an invalid notification message'
          end

          #Count notification messages received
          counter += 1
          break if counter > 6
        end
        raise 'Expected notification message not received' if records.include?(false)
      end
      Logger.new(STDOUT).info("All notification messages received from the #{@notification_topic} topic")
    rescue Timeout::Error
      raise "Timed out waiting for messages from the #{@notification_topic} topic. Expected 6 messages, but only received #{counter - 1}."
    end
  end

  it 'should stop sending messages to the output topic when a termination notification message is received' do
    batch_template = {
        name: "hri-flink-validation-passthrough-#{@git_branch}-terminated-batch-name",
        dataType: 'hri-flink-validation-passthrough-batch',
        topic: @input_topic
    }
    @batch_id = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    records_before_terminate = 10
    offset = (@event_streams_helper.consumer_group_current_offset("hri-validation-#{@input_topic}-#{@output_topic}", @input_topic) || 0)
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      if key <= records_before_terminate
        @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
        @flink_job.kafka_producer.deliver_messages
      else
        Timeout.timeout(15, nil, "Offset mismatch with consumer group: hri-validation-#{@input_topic}-#{@output_topic}") do
          while true
            current_offset = @event_streams_helper.consumer_group_current_offset("hri-validation-#{@input_topic}-#{@output_topic}", @input_topic)
            break if offset + records_before_terminate == current_offset
          end
        end
        response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id, 'terminate', nil, {'Authorization' => "Bearer #{@hri_oauth_token}"})
        raise "Failed to update the status of batch ID #{@batch_id} to terminated" unless response.code == 200
        break
      end
      key += 1
    end
    Logger.new(STDOUT).info("'terminated' notification sent to the #{@notification_topic} topic")

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, records_before_terminate)

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started terminated))
  end

  it 'should send any records with an unknown batch ID to the invalid topic' do
    invalid_batch_id = 'rspecUnknownBatch'
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
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: invalid_batch_id})
      @flink_job.kafka_producer.deliver_messages
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    @record_validator.all_invalid_records(@kafka_invalid_consumer, @invalid_topic, invalid_batch_id, key - 1, true)

    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted))
  end

  it 'should fail the batch if more records than expected are received before the batch status is sendCompleted' do
    expected_record_count = {
        expectedRecordCount: 5
    }
    batch_template = {
        name: "hri-flink-validation-passthrough-#{@git_branch}-valid-batch-name",
        dataType: 'hri-flink-validation-passthrough-batch',
        topic: @input_topic
    }
    @batch_id = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)
    sleep 5

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
      @flink_job.kafka_producer.deliver_messages
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, key - 1)

    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted failed))
  end

  it 'should fail the batch if more records than expected are received after the batch status is sendCompleted but within the timeout window' do
    batch_name = "hri-flink-validation-passthrough-#{@git_branch}-valid-batch"
    batch_data_type = 'hri-flink-validation-passthrough-batch'
    expected_record_count = {
        expectedRecordCount: 15
    }
    batch_template = {
        name: batch_name,
        dataType: batch_data_type,
        topic: @input_topic,
    }
    @batch_id = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id, testUtf8あいうえおか: 'あいうえおか'})
      @flink_job.kafka_producer.deliver_messages
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200
    sleep 2

    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id, testUtf8あいうえおか: 'あいうえおか'})
      @flink_job.kafka_producer.deliver_messages
      key += 1
      break if key == 17
    end
    Logger.new(STDOUT).info("Additional test messages sent to the #{@input_topic} topic")

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, key - 1)

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted failed))
  end

  it 'should not fail the batch if more records than expected are received after the batch status is sendCompleted and the timeout window has expired' do
    batch_name = "hri-flink-validation-passthrough-#{@git_branch}-valid-batch"
    batch_data_type = 'hri-flink-validation-passthrough-batch'
    expected_record_count = {
        expectedRecordCount: 15
    }
    batch_template = {
        name: batch_name,
        dataType: batch_data_type,
        topic: @input_topic,
    }
    @batch_id = @mgmt_api_helper.create_batch(TENANT_ID, batch_template, @hri_oauth_token)

    key = 1
    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
      @flink_job.kafka_producer.deliver_messages
      key += 1
    end
    Logger.new(STDOUT).info("Test messages sent to the #{@input_topic} topic")

    response = @mgmt_api_helper.hri_put_batch(TENANT_ID, @batch_id, 'sendComplete', expected_record_count, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to update the status of batch ID #{@batch_id} to sendCompleted" unless response.code == 200

    @record_validator.all_output_records(@kafka_output_consumer, @output_topic, @batch_id, key - 1)

    @record_validator.all_notification_records(@kafka_notification_consumer, @notification_topic, @batch_id, %w(started sendCompleted completed), BATCH_COMPLETION_DELAY)

    # Give the Flink job time to process the completed message
    sleep 1

    File.readlines(File.join(File.dirname(__FILE__), "../test_data/mixed_records.txt")).each do |line|
      @flink_job.kafka_producer.produce(line, key: "#{key}", topic: @input_topic, headers: {batchId: @batch_id})
      @flink_job.kafka_producer.deliver_messages
      key += 1
    end
    Logger.new(STDOUT).info("Additional test messages sent to the #{@input_topic} topic")

    @record_validator.all_invalid_records(@kafka_invalid_consumer, @invalid_topic, @batch_id, key - 16)

    #Verify that the batch is not failed
    response = @mgmt_api_helper.hri_get_batch(TENANT_ID, @batch_id, {'Authorization' => "Bearer #{@hri_oauth_token}"})
    raise "Failed to get batch with ID #{@batch_id} from the management API" unless response.code == 200
    raise "Invalid batch status. Expected: completed, Received: #{JSON.parse(response.body)['status']}" unless JSON.parse(response.body)['status'] == 'completed'
  end

end
