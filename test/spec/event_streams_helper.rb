# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

class EventStreamsHelper

  def initialize
    @helper = Helper.new
  end

  def create_topic(topic, partitions)
    unless get_topics.include?(topic)
      @helper.exec_command("bx es topic-create #{topic} -p #{partitions}")
      Logger.new(STDOUT).info("Topic #{topic} created.")
    end
  end

  def delete_topic(topic)
    if get_topics.include?(topic)
      @helper.exec_command("bx es topic-delete #{topic} -f")
      Logger.new(STDOUT).info("Topic #{topic} deleted.")
    end
  end

  def get_groups
    @helper.exec_command("bx es groups")
  end

  def reset_consumer_group(existing_groups, group, topic, mode)
    if existing_groups.include?(group)
      if @helper.exec_command("bx es group #{group}").include?(topic)
        Timeout.timeout(10) do
          while @helper.exec_command("bx es group-reset #{group} --mode #{mode} --topic #{topic} --execute").split("\n").last != 'OK' &&
            @helper.exec_command("bx es group #{group}").split("\n").select { |s| s.include?(topic) }[0].split(' ')[4].to_i != 0
            Logger.new(STDOUT).warn("Failed to reset consumer group #{group} to the latest offset")
            sleep 0.1
          end
        end
        Logger.new(STDOUT).info("Consumer group #{group} reset to the latest offset")
      end
    end
  end

  def consumer_group_current_offset(group, topic)
    if get_groups.include?(group)
      @helper.exec_command("bx es group #{group}").split("\n").select { |s| s.start_with?(topic) }[0].split(' ')[2].to_i
    end
  end

  def verify_topic_creation(expected_topics)
    Timeout.timeout(60, nil, 'Kafka topics not created after 60 seconds') do
      topics = get_topics
      until (topics & expected_topics).sort == expected_topics.sort
        sleep 1
        topics = get_topics
      end
    end
  end

  private

  def get_topics
    @helper.exec_command("bx es topics").split("\n").map { |t| t.strip }
  end

end