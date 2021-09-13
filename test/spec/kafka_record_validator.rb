# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

class KafkaRecordValidator

  def single_output_record(message, record_number, batch_id, validate_key = false)
    #Verify that message headers are preserved
    message.headers.each do |key, value|
      if key == 'batchId'
        raise "Output message contains an incorrect batch ID. Expected: #{batch_id}, Received: #{value}" unless value == batch_id
      elsif key.dup.force_encoding('UTF-8') == 'testUtf8あいうえおか'
        raise "Output message contains an incorrect header. Expected: 'あいうえおか', Received: #{value.dup.force_encoding('UTF-8')}" unless value.dup.force_encoding('UTF-8') == 'あいうえおか'
      else
        raise "An invalid message header was received: #{key}:#{value}"
      end
    end

    #Verify that the message body is preserved
    parsed_value = JSON.parse(message.value)
    raise "Output message contains an incorrect resourceType. Expected: Practitioner, Received: #{parsed_value['entry'][0]['resource']['resourceType']}" unless parsed_value['entry'][0]['resource']['resourceType'] == 'Practitioner'
    raise "Output message contains an incorrect Practitioner name. Expected name to include 'Dr. Test', Received: #{parsed_value['entry'][0]['resource']['name'][0]['given'][0]}" unless parsed_value['entry'][0]['resource']['name'][0]['given'][0].include?('Dr. Test')

    #Verify that the message key is preserved
    if validate_key
      raise "Output message contains an incorrect key. Expected: #{record_number}, Received: #{message.key.to_i}" unless message.key.to_i == record_number
    end
  end

  def all_notification_records(consumer, notification_topic, batch_id, expected_messages, batch_completion_delay = nil, batch_info = nil)
    counter = 1
    messages = []
    begin
      Timeout.timeout(60) do
        consumer.each_message do |message|

          parsed_data = JSON.parse(message.value)
          raise "Notification message contains an incorrect batch ID. Expected: #{batch_id}, Received: #{parsed_data['id']}" unless parsed_data['id'] == batch_id

          case counter
            when 1..expected_messages.size
              messages << { status: parsed_data['status'], create_time: message.create_time.to_time.to_i }
            else
              raise "More than #{expected_messages.size} notification messages received"
          end

          if batch_info
            raise "Notification message contains an incorrect dataType. Expected: #{batch_info[:batch_data_type]}, Received: #{parsed_data['dataType']}" unless parsed_data['dataType'] == batch_info[:batch_data_type]
            raise "Notification message contains an incorrect batch name. Expected: #{batch_info[:batch_name]}, Received: #{parsed_data['name']}" unless parsed_data['name'] == batch_info[:batch_name]
            raise 'Notification message contains incorrect batch metadata' unless parsed_data['metadata']['test'] == batch_info[:batch_metadata]
            if %w(sendCompleted completed).include?(parsed_data['status'])
              raise "Notification message contains an incorrect expectedRecordCount. Expected: #{batch_info[:expectedRecordCount]}, Received: #{parsed_data['expectedRecordCount']}" unless parsed_data['expectedRecordCount'] == batch_info[:expectedRecordCount]
              raise "Notification message contains an incorrect recordCount. Expected: #{batch_info[:expectedRecordCount]}, Received: #{parsed_data['recordCount']}" unless parsed_data['recordCount'] == batch_info[:expectedRecordCount]
            end
          end

          #Count notification messages received
          counter += 1
          break if counter > expected_messages.size
        end
      end

      message_statuses = messages.collect { |message| message[:status] }
      raise "Invalid notification messages received. Expected: #{expected_messages}, Received: #{message_statuses}" unless message_statuses == expected_messages
      Logger.new(STDOUT).info("All notification messages received from the #{notification_topic} topic")

      if batch_completion_delay && (message_statuses & %w(started sendCompleted) == %w(started sendCompleted))
        send_completed_time = messages.select { |message| message[:status] == 'sendCompleted' }[0][:create_time]
        completed_time = messages.select { |message| message[:status] == 'completed' }[0][:create_time]
        raise "Batch completion delay is #{batch_completion_delay}ms, but the delay between sendCompleted and completed is only #{(completed_time - send_completed_time) * 1000}ms" unless (completed_time - send_completed_time) >= (batch_completion_delay / 1000)
      end

    rescue Timeout::Error
      raise "Timed out waiting for messages from the #{notification_topic} topic. Expected #{expected_messages.size} messages, but only received #{counter - 1}."
    end
  end

  def all_output_records(consumer, output_topic, batch_id, expected_messages, validate_key = false, timeout = 60)
    counter = 1
    begin
      Timeout.timeout(timeout) do
        consumer.each_message do |message|
          Logger.new(STDOUT).info("Message #{counter} of #{expected_messages} received")

          single_output_record(message, counter, batch_id, validate_key)

          #Count messages received
          counter += 1
          break if counter > expected_messages
        end
      end
      Logger.new(STDOUT).info("All test messages received from the #{output_topic} topic")
    rescue Timeout::Error
      raise "Timed out waiting for messages from the #{output_topic} topic. Expected #{expected_messages} messages, but only received #{counter - 1}."
    end
  end

  def all_invalid_records(consumer, invalid_topic, batch_id, expected_messages, validate_error_message = false)
    counter = 1
    begin
      Timeout.timeout(60) do
        consumer.each_message do |message|
          Logger.new(STDOUT).info("Message #{counter} of #{expected_messages} received")

          #Verify that message headers are preserved
          raise "Invalid message contains an incorrect batch ID. Expected: #{batch_id}, Received: #{message.headers['batchId']}" unless message.headers['batchId'] == batch_id

          #Verify the failure message
          if validate_error_message
            raise "Invalid batch failure message received. Expected: 'Bad Message - Unknown batchId', Received: #{JSON.parse(message.value)['failure']}" unless JSON.parse(message.value)['failure'] == 'Bad Message - Unknown batchId'
          end

          #Count messages received
          counter += 1
          break if counter > expected_messages
        end
      end
      Logger.new(STDOUT).info("All test messages received from the #{invalid_topic} topic")
    rescue Timeout::Error
      raise "Timed out waiting for messages from the #{invalid_topic} topic. Expected #{expected_messages} messages, but only received #{counter}."
    end
  end

end