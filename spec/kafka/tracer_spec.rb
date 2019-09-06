# frozen_string_literal: true

require 'uri'
require 'kafka'
require 'opentracing'

RSpec.describe Kafka::Tracer do
  describe '#patch_client_deliver_message' do
    let(:client) { Kafka::Client.new(seed_brokers: ['localhost']) }
    after do
      Kafka::Tracer.remove
    end

    it 'starts a span for the message' do
      tracer = double(start_active_span: true)
      Kafka::Tracer.instrument(tracer: tracer)
      client.deliver_message('hello', headers: {}, topic: 'test')
      expect(tracer).to have_received(:start_active_span)
    end

    it 'can be configured to skip creating a span for some messages' do
      tracer = double(start_active_span: true)
      allow(client).to receive(:deliver_message_original)

      Kafka::Tracer.instrument(
        tracer: tracer,
        ignore_message: ->(_value, _key, _headers, topic, _, _) { topic == 'ignore' }
      )

      client.deliver_message('hello', headers: {}, topic: 'ignore')
      expect(tracer).not_to have_received(:start_active_span)
      expect(client).to have_received(:deliver_message_original)

      client.deliver_message('hello', headers: {}, topic: 'test')
      expect(tracer).to have_received(:start_active_span)
    end

    it 'follows semantic conventions for the span tags' do
      tracer = double(start_active_span: true)
      Kafka::Tracer.instrument(tracer: tracer)

      client.deliver_message('hello', headers: {}, topic: 'testing')

      expect(tracer).to have_received(:start_active_span).with(
        'kafka.producer',
        tags: {
          'component' => 'ruby-kafka',
          'span.kind' => 'producer',
          'message_bus.partition' => nil,
          'message_bus.partition_key' => nil,
          'message_bus.destination' => 'testing',
          'message_bus.pending_message' => false
        }
      )
    end

    it 'tags the span as an error when the response is an error' do
      error = Kafka::Error.new('invalid format')

      span = double(set_tag: true, context: nil)
      scope = double(span: span)
      tracer = double(start_active_span: true)
      allow(tracer).to receive(:start_active_span).and_yield(scope)

      Kafka::Tracer.instrument(tracer: tracer)

      allow(client).to receive(:deliver_message_original).and_raise(error)

      expect do
        client.deliver_message('hello', headers: {}, topic: 'testing')
      end.to raise_error(Kafka::Error)

      expect(span).to have_received(:set_tag).with('error', true)
    end
  end

  describe '#patch_producer_produce' do
    let(:producer) do
      Kafka::Producer.new(
        cluster: nil,
        transaction_manager: nil,
        logger: nil,
        instrumenter: nil,
        compressor: nil,
        ack_timeout: nil,
        required_acks: nil,
        max_retries: nil,
        retry_backoff: nil,
        max_buffer_size: nil,
        max_buffer_bytesize: nil
      )
    end

    after do
      Kafka::Tracer.remove
    end

    it 'starts a span for the message' do
      tracer = double(start_active_span: true)
      Kafka::Tracer.instrument(tracer: tracer)
      producer.produce('hello', headers: {}, topic: 'test')
      expect(tracer).to have_received(:start_active_span)
    end

    it 'can be configured to skip creating a span for some messages' do
      tracer = double(start_active_span: true)

      Kafka::Tracer.instrument(
        tracer: tracer,
        ignore_message: ->(_value, _key, _headers, topic, _, _) { topic == 'ignore' }
      )
      allow(producer).to receive(:produce_original)

      producer.produce('hello', headers: {}, topic: 'ignore')
      expect(tracer).not_to have_received(:start_active_span)

      producer.produce('hello', headers: {}, topic: 'test')
      expect(tracer).to have_received(:start_active_span)
      expect(producer).to have_received(:produce_original)
    end

    it 'follows semantic conventions for the span tags' do
      tracer = double(start_active_span: true)
      Kafka::Tracer.instrument(tracer: tracer)

      producer.produce('hello', headers: {}, topic: 'testing')

      expect(tracer).to have_received(:start_active_span).with(
        'kafka.producer',
        tags: {
          'component' => 'ruby-kafka',
          'span.kind' => 'producer',
          'message_bus.partition' => nil,
          'message_bus.partition_key' => nil,
          'message_bus.destination' => 'testing',
          'message_bus.pending_message' => true
        }
      )
    end

    it 'tags the span as an error when the response is an error' do
      error = Kafka::Error.new('invalid format')

      span = double(set_tag: true, context: nil)
      scope = double(span: span)
      tracer = double(start_active_span: true)
      allow(tracer).to receive(:start_active_span).and_yield(scope)

      Kafka::Tracer.instrument(tracer: tracer)

      allow(producer).to receive(:produce_original).and_raise(error)

      expect do
        producer.produce('hello', headers: {}, topic: 'testing')
      end.to raise_error(Kafka::Error)

      expect(span).to have_received(:set_tag).with('error', true)
    end
  end

  describe '#patch_client_each_message' do
    let(:client) { Kafka::Client.new(seed_brokers: ['localhost']) }
    after do
      Kafka::Tracer.remove
    end

    it 'extracts the context from the message headers' do
      tracer = double(start_active_span: true)
      message = instance_double(Kafka::FetchedMessage,
                                headers: {},
                                partition: nil,
                                topic: 'test')
      allow(tracer).to receive(:extract)
      allow(client).to receive(:each_message_original).and_yield(message)
      Kafka::Tracer.instrument(tracer: tracer)

      client.each_message(topic: 'test') { ; }

      expect(tracer).to have_received(:extract)
    end

    it 'starts a new span with the parent context' do
      tracer = double(start_active_span: true)
      context = double
      reference = double
      message = instance_double(Kafka::FetchedMessage,
                                headers: {},
                                partition: "A",
                                topic: 'test')
      allow(tracer).to receive(:extract).and_return(context)
      allow(client).to receive(:each_message_original).and_yield(message)
      allow(OpenTracing::Reference).to receive(:follows_from).and_return(reference)
      Kafka::Tracer.instrument(tracer: tracer)

      client.each_message(topic: 'test') { ; }

      expect(tracer).to have_received(:start_active_span).with(
        'kafka.consumer',
        references: [reference],
        tags: {
          'component' => 'ruby-kafka',
          'span.kind' => 'consumer',
          'message_bus.partition' => 'A',
          'message_bus.destination' => 'test',
          'message_bus.pre_fetched_in_batch' => true
        })
    end

    it 'tags the span as an error when the consumer errors' do
      span = double(set_tag: nil)
      scope = double(span: span)
      tracer = double
      allow(tracer).to receive(:start_active_span).and_yield(scope)
      message = instance_double(Kafka::FetchedMessage,
                                headers: {},
                                partition: nil,
                                topic: 'test')
      allow(tracer).to receive(:extract)
      allow(client).to receive(:each_message_original).and_yield(message)
      Kafka::Tracer.instrument(tracer: tracer)
      error = StandardError.new("stop")

      expect do
        client.each_message(topic: 'test') { raise error }
      end.to raise_error(error)

      expect(span).to have_received(:set_tag).with('error', true)
    end
  end

  describe '#patch_consumer_each_message' do
    let(:consumer) do
      Kafka::Consumer.new(
        cluster: nil,
        logger: nil,
        instrumenter: nil,
        group: nil,
        fetcher: nil,
        offset_manager: nil,
        session_timeout: nil,
        heartbeat: nil
      )
    end
    after do
      Kafka::Tracer.remove
    end

    it 'extracts the context from the message headers' do
      tracer = double(start_active_span: true)
      message = instance_double(Kafka::FetchedMessage,
                                headers: {},
                                partition: nil,
                                topic: 'test')
      allow(tracer).to receive(:extract)
      allow(consumer).to receive(:each_message_original).and_yield(message)
      Kafka::Tracer.instrument(tracer: tracer)

      consumer.each_message { ; }

      expect(tracer).to have_received(:extract)
    end

    it 'starts a new span with the parent context' do
      tracer = double(start_active_span: true)
      context = double
      reference = double
      message = instance_double(Kafka::FetchedMessage,
                                headers: {},
                                partition: "A",
                                topic: 'test')
      allow(tracer).to receive(:extract).and_return(context)
      allow(consumer).to receive(:each_message_original).and_yield(message)
      allow(OpenTracing::Reference).to receive(:follows_from).and_return(reference)
      Kafka::Tracer.instrument(tracer: tracer)

      consumer.each_message { ; }

      expect(tracer).to have_received(:start_active_span).with(
        'kafka.consumer',
        references: [reference],
        tags: {
          'component' => 'ruby-kafka',
          'span.kind' => 'consumer',
          'message_bus.partition' => 'A',
          'message_bus.destination' => 'test',
          'message_bus.pre_fetched_in_batch' => true
        })
    end

    it 'tags the span as an error when the consumer errors' do
      span = double(set_tag: nil)
      scope = double(span: span)
      tracer = double
      allow(tracer).to receive(:start_active_span).and_yield(scope)
      message = instance_double(Kafka::FetchedMessage,
                                headers: {},
                                partition: nil,
                                topic: 'test')
      allow(tracer).to receive(:extract)
      allow(consumer).to receive(:each_message_original).and_yield(message)
      Kafka::Tracer.instrument(tracer: tracer)
      error = StandardError.new("stop")

      expect do
        consumer.each_message { raise error }
      end.to raise_error(error)

      expect(span).to have_received(:set_tag).with('error', true)
    end
  end
end
