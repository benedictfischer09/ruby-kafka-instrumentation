# Kafka::Instrumentation

Open Tracing instrumentation for the [kafka gem](https://github.com/zendesk/ruby-kafka). By default it starts a new span for every message written to kafka and propogates the span context when any consumer consumes the message. It follows the open tracing tagging [semantic conventions](https://opentracing.io/specification/conventions)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'ruby-kafka-instrumentation'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install ruby-kafka-instrumentation

## Usage
First load the instrumentation (Note: this won't automatically instrument the http gem)
```
require "kafka/instrumentation"
```

If you have setup `OpenTracing.global_tracer` you can turn on spans for all requests with just:
```
    Kafka::Instrumentation.instrument
```

If you need more control over the tracer or which requests get their own span you can configure both settings like:
```
    Kafka::Instrumentation.instrument(
        tracer: tracer,
        ignore_message: ->(value, key, headers, topic, partition, partition_key) { topic == 'testing' }
    )
```

## Development

After checking out the repo, run `bundle install` to install dependencies. Then, run `rspec` to run the tests.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/benedictfischer09/ruby-kafka-instrumentation. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Kafka::Instrumentation project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/benedictfischer09/ruby-kafka-instrumentation/blob/master/CODE_OF_CONDUCT.md).
