require 'queueprocessor/version'
require 'yaml'
require 'pg'
require 'base64'
require 'logger'
require 'thread'
require 'socket'
require 'json'

module QueueProcessor

  class Event
    attr_accessor :dbid, :data

    def initialize( data = {} )
      @data = data
      @dbid = nil
    end

    def to_s 
      @data.to_s 
    end

    def []( key ) 
      @data[key] 
    end

    # create safe string representation of event struct
    def serialize 
      Base64.encode64(@data.to_yaml) 
    end
    
    # parse string representation of event
    def deserialize( eventdata )
      @data = YAML.load( Base64.decode64(eventdata) )
      return self
    end
  end

  class DBError < StandardError; end

  class GenericQueue; end

  class PGQueue < GenericQueue
    # TODO error handling
    # TODO sort

    # event record statuses
    EV_READY   = 1
    EV_LOCKED   = 2
    EV_PROCESSED   = 3

    def status?
      @conn && @conn.status == PG::CONNECTION_OK
    end

    def initialize( db = {} )
      @connected  = false
      @db = db
      if self.connect && @conn.status == PG::CONNECTION_OK
        @connected = true
      else
        raise DBError, "Bad connection status"
      end
    rescue
      raise DBError, "Connection error " + $!.to_s
    end

    # get #{count} elements to process, locking them
    def getnext( queueid, count = 1 )
      events = []
      @conn.transaction do |conn|
        response = @conn.exec("select eventid, event from queue where queueid=#{queueid} and status=#{EV_READY} limit #{count} for update nowait"); 
        response.each do |eventdata|
          event = Event.new().deserialize( eventdata['event'] );
          event.dbid = eventdata['eventid']
          events.push( event )
        end
        @conn.exec("update queue set status=#{EV_LOCKED} where eventid in(#{ events.map{|event| event.dbid }.join(",")})") if events.length > 0
      end
      return events
    rescue
      p "Lock error "+ $!.to_s
      false
#      raise DBError, "Select error " + $!.to_s
    end  
      
    # call processing for the given events list and release locks  
    def process( events )
      successfull = []
      unsuccessfull = []
      events.each do |event|  
        begin
          if yield event  
            # should be commented to keep processed records
#            @conn.exec("delete from queue where eventid=#{event.dbid}")
            successfull.push( event.dbid )
          else
            unsuccessfull.push( event.dbid )
          end
        rescue
          unsuccessfull.push( event.dbid )
        end
      end
      @conn.exec("update queue set status=#{EV_PROCESSED} where eventid in (#{ successfull.join(?,) })") if successfull.length > 0
      @conn.exec("update queue set status=#{EV_READY} where eventid in (#{ unsuccessfull.join(?,) })") if unsuccessfull.length > 0
    end

    # add QueueProcessor::Event to queue
    def put( queueid, event )
      if @connected 
        res = @conn.exec("insert into queue (queueid, event, status ) values ( #{queueid}, '#{event.serialize}', #{EV_READY} )")
      else
        raise DBError, "Not connected"
      end
    end

    def connect
      @conn = PG::Connection.open( @db )
    end
  end

  # class for db connection pool used as a semaphore
  class DBConnectionPool
    def initialize( db, maxnum)
      @maxnum = maxnum
      @pool = []
      @lock = Mutex.new
      @db = db
    end

    def getconn
      @lock.synchronize do
        if @pool.length < @maxnum
          @pool.push(
            {
              :conn => QueueProcessor::PGQueue.new(@db ),
              :status => :occupied 
            }
          )
          return [@pool.length - 1, @pool.last[:conn]]
        else
          #lookup free
          (0..@pool.length-1).each do |i|
            if @pool[i][:status] == :free
              @pool[i][:status] = :occupied
              return[i,@pool[i][:conn]]
            end
          end
          # no free connections
          return nil
        end
      end
    end

    def releaseconn(i)
      @lock.synchronize { @pool[i][:status] = :free }
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

  class PGQueueProcessor 
    def initialize( settings, db )
      @db = db
      @settings = settings
      @mainconnection = PGQueue.new( @db)
      @status = {}
      @lock = Mutex.new
      @need_to_restart = {}
      @settings.each_pair do |queueid, qsettings| 
        @status[queueid]={ :workers => 0, :status=> 'idle' }
        @need_to_restart[queueid] = false
      end
    end

    def put(queueid, data) 
      @mainconnection.put(queueid, Event.new(data))
    rescue
      p "no connection #{$!}"
    end
  
    def workersrun
      @settings.each_key do |queueid|
        Thread.new do 
          loop do
            qsettings = @settings[queueid]
            connections = DBConnectionPool.new( @db, qsettings[:workers])
            @status[queueid][:workers] = qsettings[:workers]
            loop do
              (cid,qp) = connections.getconn
              # reread settings
              @lock.synchronize do
                if @need_to_restart[queueid]
                  @need_to_restart[queueid] = false
                  break # restart queue processing
                end
              end
              if cid
                events = qp.getnext(queueid, qsettings[:frame])
                if( events.length > 0 )
                  @status[queueid][:status] = 'working'
                  Thread.new(events,cid,qp) do |events,cid,qp| 
                    begin
                      qp.process(events) do |event| 
                        sleep(0.001)
                      end
                    ensure
                      connections.releaseconn(cid)
                    end
                  end
                else
                  @status[queueid][:status] = 'idle'
                  connections.releaseconn(cid)
                  sleep(0.001)
                end
              else
                sleep(0.001)
              end
            end
          end
        end
      end
    end

    # runs workers and tcp server to control
    def masterrun
      self.workersrun 
      self.serverrun 
    end
    
    # tcp server
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
p request
            req_hash = JSON.parse( request )
            if( req_hash['set'] )
              req_hash['set'].each do |rec|
                @settings[res['queueid']][:workers] = res['workers']
                @settings[res['queueid']][:frame] = res['frame']
		@need_to_restart[res['queueid']] = true
              end
            end
            response = "OK"
            if( req_hash['get'] == 'stats' )
              response = JSON.generate( @status )
            end
            socket.print "HTTP/1.1 200 OK\r\n" +
              "Content-Type: text/plain\r\n" +
              "Content-Length: #{response.bytesize}\r\n" +
              "Connection: close\r\n\r\n#{response}"
            socket.close
          end
        rescue IO::WaitReadable
          sleep 0.1
        rescue 
          p "tcp server error #{$!}"
        end
      end
    end
    
  end
end

