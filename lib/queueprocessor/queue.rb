require 'queueprocessor/version'
require 'yaml'
require 'pg'
require 'base64'
require 'logger'
require 'thread'
require 'socket'
require 'json'

module QueueS

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

  # Queue, based on Postgres. 
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
    def process( events, process_stat )
      successfull = []
      unsuccessfull = []
      process_stat.start( events.length )
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
      process_stat.finish( successfull.length, unsuccessfull.length )
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

  # class for db connection pool also used as a semaphore
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
              :conn => QueueS::PGQueue.new(@db ),
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

  class ProcessStats
    def initialize
      @succ = 0     #successful
      @unsucc = 0   #unsuccessful
      @time = 0     #time to process
      @eps = 0      # event per second
    end

    def start( totalev )
      @totalev = totalev
      @starttime = Time.now()
    end

    def finish( succ, unsucc )
      @succ = succ
      @unsucc = unsucc
      @time = Time.now() - @starttime
      @eps = ( @time > 0 ) ? ( @succ + @unsucc ) / @time : 0
      self
    end
    def to_s
      "Report: #{@succ} (Good) + #{@unsucc} (Bad). Eps: #{@eps}"
    end
  end
      

end

