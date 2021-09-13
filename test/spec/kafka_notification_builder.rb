# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

class KafkaNotificationBuilder

  def started_notification(batch_id, name, input_topic, invalid_threshold = -1)
    {
        id: batch_id,
        name: name,
        topic: input_topic,
        dataType: 'claims',
        status: 'started',
        invalidThreshold: invalid_threshold,
        startDate: DateTime.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
  end

  def send_completed_notification(batch_id, name, input_topic, record_count)
    {
        id: batch_id,
        name: name,
        topic: input_topic,
        dataType: 'claims',
        status: 'sendCompleted',
        expectedRecordCount: record_count,
        startDate: DateTime.now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        endDate: DateTime.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
  end

  def terminated_notification(batch_id, name, input_topic)
    {
        id: batch_id,
        name: name,
        topic: input_topic,
        dataType: 'claims',
        status: 'terminated',
        startDate: DateTime.now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        endDate: DateTime.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
  end

end