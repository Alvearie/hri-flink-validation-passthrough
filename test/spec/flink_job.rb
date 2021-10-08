
require_relative '../env'

class FlinkJob

  @@total_jobs = 0
  @@total_batches = 0

  def initialize(flink_helper, event_streams_helper, kafka_helper, validation_jar_id, tenant)
    @flink_helper = flink_helper
    @event_streams_helper = event_streams_helper
    @kafka_helper = kafka_helper
    @validation_jar_id = validation_jar_id
    @tenant = tenant
    @@total_jobs += 1
    @job_number = @@total_jobs

    @job_started = false
    @flink_monitor_lock = Mutex.new
  end

  attr_reader :kafka_input_topic
  attr_reader :kafka_producer

  def start_job(flink_oauth_token, batch_complete_delay, input_topic)
    @kafka_input_topic = input_topic
    Logger.new(STDOUT).info("Starting Job #{@job_number}")
    @kafka_producer = @kafka_helper.producer(compression_codec: :zstd)

    # Start the flink job and verify that it is running
    @job_id = @flink_helper.start_flink_job(@validation_jar_id, ENV['KAFKA_BROKERS'], ENV['SASL_PLAIN_PASSWORD'], @kafka_input_topic, ENV['HRI_SERVICE_URL'], ENV['OIDC_HRI_INTERNAL_CLIENT_ID'], ENV['OIDC_HRI_INTERNAL_CLIENT_SECRET'], "#{ENV['APPID_URL']}/oauth/v4/#{ENV['APPID_TENANT']}", ENV['APPID_HRI_AUDIENCE'], batch_complete_delay, flink_oauth_token)
    @flink_helper.verify_job_state(@job_id, flink_oauth_token, 'RUNNING')
    @job_started = true

    # wait for the job to completely start up, otherwise it will miss some of the initial messages.
    sleep(5)

    @flink_monitor_thread = Thread.new {
      Thread.current.abort_on_exception = false
      Thread.current.report_on_exception = false
      flink_monitor_job(flink_oauth_token)
    }

    Logger.new(STDOUT).info("Finished starting Job #{@job_number}")
  end

  def flink_monitor_job(flink_oauth_token)
    loop do
      @flink_monitor_lock.lock

      flink_job_exceptions = retrieve_flink_errors(flink_oauth_token)
      raise "#{flink_job_exceptions['root-exception']}" if flink_job_exceptions['root-exception']

      flink_checkpoints = retrieve_flink_checkpoints(flink_oauth_token)
      raise "One or more checkpoints failed" if flink_checkpoints['counts']['failed'] > 0

      @flink_monitor_lock.unlock

      # Only check flink job every 5 seconds
      sleep(5)

    ensure
      @flink_monitor_lock.unlock if @flink_monitor_lock.owned?
    end
  end

  def retrieve_flink_errors(flink_oauth_token)
    flink_job_exceptions_response = @flink_helper.get_job_exceptions(@job_id, {'Authorization' => "Bearer #{flink_oauth_token}"})
    raise 'Failed to get Flink job exceptions' unless flink_job_exceptions_response.code == 200
    JSON.parse(flink_job_exceptions_response)
  end

  def retrieve_flink_checkpoints(flink_oauth_token)
    flink_job_checkpoints_response = @flink_helper.get_job_checkpoints(@job_id, {'Authorization' => "Bearer #{flink_oauth_token}"})
    raise 'Failed to get Flink job checkpoints' unless flink_job_checkpoints_response.code == 200
    JSON.parse(flink_job_checkpoints_response)
  end

  # Thread safe. Returns true if the flink pipeline failed
  def failed?
    !@flink_monitor_thread.alive?
  end

  def stop_flink_job(flink_oauth_token)
    if @job_started
      response = @flink_helper.stop_job(@job_id, {'Authorization' => "Bearer #{flink_oauth_token}"})
      raise "Failed to stop Flink job with ID: #{@job_id}" unless response.code == 202
      begin
        @flink_helper.verify_job_state(@job_id, flink_oauth_token, 'FINISHED')
        Logger.new(STDOUT).info("Stopped Flink job #{@job_id}")
        @job_started = false
      rescue
        (Timeout::Error)
        Logger.new(STDOUT).warn('Timeout stopping flink job')
      end
    end
  end

  def stop_job(flink_oauth_token)
    # Get the locks on the mutexes used by the monitor threads. This ensures that the consumer won't be killed while it
    # is in the middle of processing an incoming message and the flink monitor will not monitor for exceptions while the
    # job is being cancelled
    @consumer_lock.lock
    @flink_monitor_lock.lock

    exception_occurred = false

    # Cancel flink job. It isn't needed anymore.
    Logger.new(STDOUT).info("Stopping Job #{@job_number}")
    stop_flink_job(flink_oauth_token)

    @kafka_producer.shutdown if @kafka_producer

    # Exit the flink thread if it is still running, or join the thread to propagate its errors
    if @flink_monitor_thread.alive?
      @flink_monitor_thread.exit
    else
      begin
        @flink_monitor_thread.join
      rescue RuntimeError => e
        Logger.new(STDOUT).error("Job#{@job_number} (#{@job_id}) encountered an error:\n#{e.message}")
        exception_occurred = true
      end
    end

    raise "Flink job#{@job_number} (#{@job_id}) failed" if exception_occurred
    Logger.new(STDOUT).info("Finished stopping Job #{@job_number}")
  end

  def cleanup_job(flink_oauth_token)
    Logger.new(STDOUT).info("Cleaning up after Job #{@job_number}")

    # If flink job wasn't previously canceled, cancel it now
    stop_flink_job(flink_oauth_token)

    Logger.new(STDOUT).info("Finished cleaning up after Job #{@job_number}")
  end

end
