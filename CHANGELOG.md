Changelog
=========

## 0.3.0 09/06/2019
  * Use follows from instead of child of semantics for consumer

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
