require 'queueprocessor/version'
require 'queueprocessor/queue'
require 'logger'
require 'thread'
require 'socket'
require 'json'

module PGQueueProcessor

  # Processor settings
  class PGQueueProcessorSettings
    attr_accessor :queueid, :workers, :frame
    def initialize( settings_hash )
      @queueid = settings_hash[:queueid]
      @workers = settings_hash[:workers]
      @frame = settings_hash[:frame]
    end

    def to_s
      "#{@queueid}: #{@workers} * #{@frame}"
    end

    def merge( qps )
      @workers = qps.workers.to_i
      @frame = qps.frame.to_i
    end
  end
    
  class PGQueueProcessorSettingsHash
    def initialize( settings_array=[] )
      @settings = {}
      settings_array.each {|record| record.instance_of?(PGQueueProcessorSettings) ? @settings[record.queueid] = record : @settings[record[:queueid].to_s] = PGQueueProcessorSettings.new(record) }
    end
    
    def []( index )
      @settings[index]
    end
    
    include Enumerable
    def each
      @settings.each_value {|record| yield record }
    end

    # QueueProcessorSettings as a parameter 
    def merge( qps )
      qps.map do |record|
        @settings[record[:queueid]].merge(record.instance_of?(PGQueueProcessorSettings) ? record : PGQueueProcessorSettings.new( record )) if @settings[record[:queueid]]
      end
    end
  end

  # main class for queue processing
  # - new( { queueid => {:workers=>1,:frame=> 1}, ...  }, pq_db_settings )
  # - put(hash) creates event
  # - run( num_of_workers, num_of_elements_per_request ) { |event| block for event processing }
  # Example:
  #     query = QueueProcessor::QueueProcessor.new( settings, { :dbname => 'queue_base'})
  #  run workers and controlling tcp server
  #     query.masterrun()
  #  run only workers
  #     query.workersrun()

  class PGQueueProcessorStats
    def initialize()
      @stats = {}
    end
    
    def to_s
      @stats.to_s
    end

    def array
      @stats
    end

    def [](queueid)
      @stats[queueid] = { :workers => 0, :status => 'idle' } unless  @stats[queueid]
      return @stats[queueid]
    end
  end

  class PGQueueProcessor 
    def initialize( settings, db )
      @db = db
      @settings = PGQueueProcessorSettingsHash.new( settings )
      @mainconnection = QueueS::PGQueue.new( @db )
      @status = PGQueueProcessorStats.new 
      @lock = Mutex.new
      @need_to_restart = {}
      @logger = Logger.new(STDERR)
      @logger.level = Logger::INFO
      @logger.debug("QueueProcessor created...")
    end

    def put(queueid, data) 
      unless( @settings[queueid] )
        raise "Unknown queue"
      end
      @mainconnection.put(queueid, QueueS::Event.new(data))
    rescue
      @logger.error("No connection while put #{$!}")
    end
    
    # runs workers and tcp server to control
    def masterrun
      self.workersrun 
      self.serverrun 
    end
    
    def workersrun
      @settings.each do |qsettings|
        Thread.new do 
          @logger.debug("Thread for "+qsettings.to_s + " started")
          loop do
            connections = QueueS::DBConnectionPool.new( @db, qsettings.workers ) 
            @status[qsettings.queueid][:workers] = qsettings.workers
            loop do
              (cid,qp) = connections.getconn
              @logger.debug("Got connection "+ cid.to_s ) if cid
              # reread settings
              @lock.synchronize do
                if @need_to_restart[qsettings.queueid]
                  @logger.info("Threads for queue #{qsettings.queueid} need to be restarted. New settings #{qsettings} ")
                  @need_to_restart[qsettings.queueid] = false
                  break # restart queue processing
                end
              end
              if cid
                events = qp.getnext( qsettings.queueid , qsettings.frame)
                @logger.info("Queue "+ qsettings.queueid.to_s  + " got " + events.length.to_s + " events")
                if( events.length > 0 )
                  @status[qsettings.queueid][:status] = 'working'
                  Thread.new(events,cid,qp) do |events,cid,qp| 
                    starttime = Time.now()
                    process_stats = QueueS::ProcessStats.new()
                    begin
                      qp.process(events, process_stats) do |event| 
                        sleep(0.0001)
                      end
                    ensure
                      @logger.error("Procesing error #{$!}") if $!
                      @logger.info("Processed " + process_stats.to_s )
                      @logger.debug("Release connection "+ cid.to_s )
                      connections.releaseconn(cid)
                    end
                  end
                else
                  @status[qsettings.queueid][:status] = 'idle'
                  @logger.debug("Release connection "+ cid.to_s )
                  connections.releaseconn(cid)
                  sleep(1)
                end
              else
                sleep(0.001)
              end
            end
          end
        end
      end
    end

    # tcp server for control
    # commands:
    #   reconfigure: { set: [ { queueid: qid, workers: new_val, frame: new_val } ]}
    #  get stats: { get: stats }
    def serverrun
      server = TCPServer.new('localhost', 2345)
      loop do
        begin
          socket = server.accept_nonblock
          if socket 
            request = socket.gets
            req_hash = JSON.parse( request )
            if( req_hash['set'] )
              req_hash['set'].each do |rec|
                @settings[rec['queueid']].merge( PGQueueProcessorSettings.new( { :queueid => rec['queueid'], :workers => rec['workers'], :frame => rec['frame']}  ))
		            @need_to_restart[rec['queueid'].to_i] = true
              end
            end
            response = "OK"
            if( req_hash['get'] == 'stats' )
              response = JSON.generate( @status.array )
            end
            socket.print "HTTP/1.1 200 OK\r\n" +
              "Content-Type: text/plain\r\n" +
              "Content-Length: #{response.bytesize}\r\n" +
              "Connection: close\r\n\r\n#{response}"
            socket.close
          end
        rescue IO::WaitReadable
          sleep(0.1)
        rescue 
          @logger.error("tcp server error #{$!}")
        end
      end
    end
    
  end
end

