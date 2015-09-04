require 'queueprocessor/version'
require 'yaml'
require 'pg'
require 'base64'
require 'logger'
require 'thread'

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
		EV_READY 	= 1
		EV_LOCKED 	= 2
		EV_PROCESSED 	= 3

		def status?
			@conn && @conn.status == PG::CONNECTION_OK
		end

		def initialize( queueid, db = {} )
			@connected  = false
			@db = db
			@queueid = queueid
			if self.connect && @conn.status == PG::CONNECTION_OK
				@connected = true
			else
				raise DBError, "Bad connection status"
			end
		rescue
			raise DBError, "Connection error " + $!.to_s
		end

		# get #{count} elements to process, locking them
		def getnext( count = 1 )
			events = []
			@conn.transaction do |conn|
				response = @conn.exec("select eventid, event from queue where queueid=#{@queueid} and status=#{EV_READY} limit #{count} for update nowait"); 
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
#			raise DBError, "Select error " + $!.to_s
		end	
			
		# call processing for the given events list and release locks	
		def process( events )
			successfull = []
			unsuccessfull = []
			events.each do |event|	
				begin
					if yield event	
						# should be commented to keep processed records
#						@conn.exec("delete from queue where eventid=#{event.dbid}")
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
		def put( event )
			if @connected 
				res = @conn.exec("insert into queue (queueid, event, status ) values ( #{@queueid}, '#{event.serialize}', #{EV_READY} )")
			else
				raise DBError, "Not connected"
			end
		end

		def connect
			@conn = PG::Connection.open( @db )
		end
	end

	# class for db connection pool used as a semaphore
	class ConnectionPool
		def initialize(queueid, db, maxnum)
			@maxnum = maxnum
			@pool = []
			@lock = Mutex.new
			@db = db
			@queueid = queueid
		end

		def getconn
			@lock.synchronize do
				if @pool.length < @maxnum
					@pool.push(
						{
							:conn => QueueProcessor::PGQueue.new(@queueid,@db ),
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
	# - new( queue id, pq_db_settings )
	# - put(hash) creates event
	# - run( num_of_workers, num_of_elements_per_request ) { |event| block for event processing }
	# Example%
	#     query = QueueProcessor::QueueProcessor.new(1,{ :dbname => 'queue_base'})
	#     query.run(5,100) { |event| sleep(0.1);}

	class PGQueueProcessor 
		def initialize(queueid, db)
			@db = db
			@queueid = queueid
			@mainconnection = PGQueue.new(@queueid, @db)
		end

		def put(data) 
			@mainconnection.put(Event.new(data))
		rescue
			p "no connection #{$!}"
		end

		def run( workers = 1, frame = 100 )

			connections = ConnectionPool.new(@queueid, @db, workers)
						
			loop do
				(cid,qp) = connections.getconn
				if cid
					events = qp.getnext(frame)
					Thread.new(events,cid,qp) do |events,cid,qp| 
						begin
							qp.process(events) do |event| 
								yield event
							end
						ensure
							connections.releaseconn(cid)
						end
					end
				else
					sleep(0.001)
				end
			end
		end
	end
end

