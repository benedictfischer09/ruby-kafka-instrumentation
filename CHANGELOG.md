Changelog
=========

## 0.4.0 09/06/2019
  * Use follows from relationship for consumer spans
  * https://opentracing.io/docs/best-practices/#tracing-message-bus-scenarios

## 0.3.0 09/06/2019
  * Yanked

## 0.2.0 08/16/2019
  * Catch load errors when kafka is not present

## 0.1.0 08/08/2019
  * Instrumention for open tracing
  * Producer apis
    - Kafka::Client#deliver_message
    - Kafka::Producer#produce
  * Consumer apis
    - Kafka::Client#each_message
    - Kafka::Consumer#each_message
