# frozen_string_literal: true

require 'kafka/tracer/version'
require 'opentracing'

module Kafka
  module Tracer
    class IncompatibleGemVersion < StandardError; end;

    class << self
      attr_accessor :ignore_message, :tracer

      IngoreMessage = ->(_value, _key, _headers, _topic, _partition, _partition_key) { false }

      def instrument(tracer: OpenTracing.global_tracer, ignore_message: IngoreMessage)
        begin
          require 'kafka'
        rescue LoadError
          return
        end
        raise IncompatibleGemVersion unless compatible_version?

        @ignore_message = ignore_message
        @tracer = tracer
        patch_producer_produce
        patch_client_deliver_message
        patch_client_each_message
        patch_consumer_each_message
      end

      def compatible_version?
        # https://github.com/zendesk/ruby-kafka/pull/604
        Gem::Version.new(Kafka::VERSION) >= Gem::Version.new("0.7.0")
      end

      def remove
        if ::Kafka::Producer.method_defined?(:produce_original)
          ::Kafka::Producer.class_eval do
            remove_method :produce
            alias_method :produce, :produce_original
            remove_method :produce_original
          end
        end

        if ::Kafka::Client.method_defined?(:deliver_message_original)
          ::Kafka::Client.class_eval do
            remove_method :deliver_message
            alias_method :deliver_message, :deliver_message_original
            remove_method :deliver_message_original
          end
        end

        if ::Kafka::Client.method_defined?(:each_message_original)
          ::Kafka::Client.class_eval do
            remove_method :each_message
            alias_method :each_message, :each_message_original
            remove_method :each_message_original
          end
        end

        if ::Kafka::Consumer.method_defined?(:each_message_original)
          ::Kafka::Consumer.class_eval do
            remove_method :each_message
            alias_method :each_message, :each_message_original
            remove_method :each_message_original
          end
        end
      end

      # used to send sync messages
      def patch_client_deliver_message
        ::Kafka::Client.class_eval do
          alias_method :deliver_message_original, :deliver_message

          def deliver_message(value, key: nil, headers: {}, topic:, partition: nil, partition_key: nil, retries: 1)
            if ::Kafka::Tracer.ignore_message.call(value, key, headers, topic, partition, partition_key)
              result = deliver_message_original(value,
                                                key: key,
                                                headers: headers,
                                                topic: topic,
                                                partition: partition,
                                                partition_key: partition_key,
                                                retries: retries)
            else
              tags = {
                'component' => 'ruby-kafka',
                'span.kind' => 'producer',
                'message_bus.partition' => partition,
                'message_bus.partition_key' => partition_key,
                'message_bus.destination' => topic,
                'message_bus.pending_message' => false
              }

              tracer = ::Kafka::Tracer.tracer

              tracer.start_active_span('kafka.producer', tags: tags) do |scope|
                OpenTracing.inject(scope.span.context, OpenTracing::FORMAT_TEXT_MAP, headers)

                begin
                  result = deliver_message_original(value,
                                                    key: key,
                                                    headers: headers,
                                                    topic: topic,
                                                    partition: partition,
                                                    partition_key: partition_key,
                                                    retries: retries)
                rescue Kafka::Error => e
                  scope.span.set_tag('error', true)
                  raise
                end
              end
            end

            result
          end
        end
      end

      # used to send a batch of messages
      def patch_producer_produce
        ::Kafka::Producer.class_eval do
          alias_method :produce_original, :produce

          def produce(value, key: nil, headers: {}, topic:, partition: nil, partition_key: nil, create_time: Time.now)

            if ::Kafka::Tracer.ignore_message.call(value, key, headers, topic, partition, partition_key)
              result = produce_original(
                value,
                key: key,
                headers: headers,
                topic: topic,
                partition: partition,
                partition_key: partition_key,
                create_time: create_time
              )

            else
              tags = {
                'component' => 'ruby-kafka',
                'span.kind' => 'producer',
                'message_bus.partition' => partition,
                'message_bus.partition_key' => partition_key,
                'message_bus.destination' => topic,
                'message_bus.pending_message' => true
              }

              tracer = ::Kafka::Tracer.tracer

              tracer.start_active_span('kafka.producer', tags: tags) do |scope|
                OpenTracing.inject(scope.span.context, OpenTracing::FORMAT_TEXT_MAP, headers)

                begin
                  result = produce_original(
                    value,
                    key: key,
                    headers: headers,
                    topic: topic,
                    partition: partition,
                    partition_key: partition_key,
                    create_time: create_time
                  )
                rescue Kafka::Error => e
                  scope.span.set_tag('error', true)
                  raise
                end
              end
            end

            result
          end
        end
      end

      # TODO
      def patch_deliver_messages; end

      def patch_client_each_message
        ::Kafka::Client.class_eval do
          alias_method :each_message_original, :each_message

          def each_message(topic:, start_from_beginning: true, max_wait_time: 5, min_bytes: 1, max_bytes: 1_048_576, &block)
            tracer = ::Kafka::Tracer.tracer

            wrapped_block = lambda { |message|
              context = tracer.extract(OpenTracing::FORMAT_TEXT_MAP, message.headers)

              tags = {
                'component' => 'ruby-kafka',
                'span.kind' => 'consumer',
                'message_bus.partition' => message.partition,
                'message_bus.destination' => message.topic,
                'message_bus.pre_fetched_in_batch' => true
              }

              tracer.start_active_span('kafka.consumer', child_of: context, tags: tags) do |scope|
                begin
                  block.call(message)
                rescue StandardError
                  scope.span.set_tag('error', true)
                  raise
                end
              end
            }

            each_message_original(
              topic: topic,
              start_from_beginning: start_from_beginning,
              max_wait_time: max_wait_time,
              min_bytes: min_bytes,
              max_bytes: max_bytes,
              &wrapped_block
            )
          end
        end
      end

      def patch_consumer_each_message
        ::Kafka::Consumer.class_eval do
          alias_method :each_message_original, :each_message

          def each_message(min_bytes: 1, max_bytes: 10485760, max_wait_time: 1, automatically_mark_as_processed: true)
            tracer = ::Kafka::Tracer.tracer

            wrapped_block = lambda { |message|
              context = tracer.extract(OpenTracing::FORMAT_TEXT_MAP, message.headers)

              tags = {
                'component' => 'ruby-kafka',
                'span.kind' => 'consumer',
                'message_bus.partition' => message.partition,
                'message_bus.destination' => message.topic,
                'message_bus.pre_fetched_in_batch' => true
              }

              tracer.start_active_span('kafka.consumer', child_of: context, tags: tags) do |scope|
                begin
                  yield message
                rescue StandardError
                  scope.span.set_tag('error', true)
                  raise
                end
              end
            }

            each_message_original(
              min_bytes: min_bytes,
              max_bytes: max_bytes,
              max_wait_time: max_wait_time,
              automatically_mark_as_processed: automatically_mark_as_processed,
              &wrapped_block
            )
          end
        end
      end
    end
  end
end
