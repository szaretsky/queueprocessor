require 'queueprocessor/version'
require 'queueprocessor/queue'
require 'logger'
require 'thread'
require 'socket'
require 'json'

module PGQueueProcessor

  # Processor settings
  class PGQueueProcessorSettings
    attr_accessor :queueid, :workers, :frame, :handler
    def initialize( settings_hash )
      @queueid = settings_hash[:queueid]
      @workers = settings_hash[:workers]
      @frame = settings_hash[:frame]
      @handler = settings_hash[:handler]
    end

    def to_s
      "#{@queueid}: #{@workers} * #{@frame}"
    end

    def merge( qps )
      @workers = qps.workers.to_i if qps.workers
      @frame = qps.frame.to_i if qps.frame
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
      @stats.map{|k,v| v['queueid'] = k; v }
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

    def put(queueid, data, connection = @mainconnection) 
      unless( @settings[queueid] )
        raise "Unknown queue"
      end
      connection.put(queueid, QueueS::Event.new(data))
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
          loop do
            begin
            @logger.info("Thread for "+qsettings.to_s + " started")
            begin
              connections = QueueS::DBConnectionPool.new( @db, qsettings.workers ) 
            rescue
              @logger.error("can not get connections #{$!}")
              sleep(1)
              break
            end
            @status[qsettings.queueid][:workers] = qsettings.workers
            loop do
              (cid,qp) = connections.getconn
              @logger.debug("Got connection "+ cid.to_s ) if cid
              # reread settings
              if @need_to_restart[qsettings.queueid]
                @logger.info("Threads for queue #{qsettings.queueid} need to be restarted. New settings #{qsettings} ")
                @need_to_restart[qsettings.queueid] = false
                break # restart queue processing
              end
              if cid
                events = qp.getnext( qsettings.queueid , qsettings.frame)
                @logger.info("Queue "+ qsettings.queueid.to_s  + " got " + events.length.to_s + " events") if events.length > 0
                if( events.length > 0 )
                  @status[qsettings.queueid][:status] = 'working'
                  Thread.new(events,cid,qp) do |events,cid,qp| 
                    starttime = Time.now()
                    process_stats = QueueS::ProcessStats.new()
                    begin
                      qp.process(events, process_stats, {:keep_processed => false }) do |event| 
                        begin
                          qsettings.handler.call(event, self, qp)
                        rescue
                          @logger.error("error while processing event #{event}")
                          false
                        end
                      end
                    ensure
                      @logger.error("Procesing error #{$!}") if $!
                      @logger.info("Processed " + process_stats.to_s )
                      @logger.debug("Release connection "+ cid.to_s )
                      @status[qsettings.queueid][:eps] = process_stats.eps
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
          rescue
            @logger.error("main queue loop error #{$!}")
            sleep(1)
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
      timer = 0
      @logger.info("tcp server starts")
      loop do
        begin
          socket = server.accept_nonblock
          if socket 
            timer = Time.now
            #TODO use read instead of gets
            until (res=socket.gets) == "\r\n" do end
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
            @logger.info("response prepaired for: #{ Time.now-timer }")
            socket.print "HTTP/1.1 200 OK\r\n" +
              "Content-Type: text/json\r\n" +
              "Content-Length: #{response.bytesize}\r\n" +
              "Connection: close\r\n\r\n#{response}"
            socket.close
            @logger.info("response sent for: #{ Time.now-timer }")
          end
        rescue IO::WaitReadable
          sleep(0.0001)
        rescue 
          @logger.error("tcp server error #{$!}") 
          @logger.error("try again") 
          sleep(1) 
        end
      end
    rescue 
      @logger.error("tcp server can not start #{$!}")
      @logger.info("runnning main loop without control server")
      loop do
        sleep(1)
      end
    end
    
  end
end

