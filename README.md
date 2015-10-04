# Queueprocessor
Queue processor with broker. Also contains built-in http server for control and statistics.

## Example

Two queues that put event to other. The first one also fails some of them.

```ruby
require 'queueprocessor'

f1 = lambda do |event,qp,conn|
  qp.put("2","Event #{event.dbid}/#{rand}", conn)
  sleep(0.1);
  0/rand(10).round
end

f2 = lambda do |event,qp,conn|
  qp.put("1","Event #{event.dbid}/#{rand}", conn)
  sleep(0.1);
end

qp = PGQueueProcessor::PGQueueProcessor.new(
  [
    {:queueid => 1, :workers => 2, :frame => 100, :handler => f1 },
    {:queueid => 2, :workers => 3, :frame => 100, :handler => f2 }
  ],{ :dbname => 'queues_development'})
qp.masterrun


20000.times { |n| qp.put("1","Event #{n}/#{rand}") }
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/queueprocessor.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

