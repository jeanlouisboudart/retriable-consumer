# retriable-consumer

We sometimes need to integrate with external systems as part our data pipeline.
Most of the time there is a Kafka Connect connectors for that see http://confluent.io/hub/.

Unfortunately we may sometimes need to integrate with an external system that is not yet supported as a connector (such as a webservice call).
In such cases you usually need to implement a retry logic around the external system call.

This repo contains a set of ideas to implement a retry logic when calling an external system.

# Build local images
This repository contains some local docker images including :

a simple producer
a retriable consumer

To build all images you just need to run :

```
docker-compose build
```

# Start the environment
To start the environment simply run the following command

```
docker-compose up -d
```

This would start :
* Zookeeper
* Kafka
* a simple producer
* a consumer with no retry
* a consumer with limited number of retries
* a consumer with infinite number of retries

Please observe the logs and behavior of each consumers.


## no-retry-consumer
```
docker-compose logs -f no-retry-consumer
```
This consumer will ignore failures in case of errors when calling an external system.
This consumer should fail if execution time (accumulated time of all calls to external system in a given poll iteration) bigger than `max.poll.interval.ms`.

For more details look at `NoRetryConsumer` class.

## limited-retries-consumer
```
docker-compose logs -f limited-retries-consumer
```
This consumer will retry X times in case of errors when calling an external system.
This consumer should fail if execution time (accumulated time of all calls to external system + retries in a given poll iteration) bigger than `max.poll.interval.ms`.

For more details look at `LimitedRetriesConsumer` class.

## infinite-retries-consumer
```
docker-compose logs -f infinite-retries-consumer
```
This consumer will retry infinitely in case of errors when calling an external system.
In case of failures, the consumer is paused and offset is set to the first element failing.
Next call to the poll(timeout) method will honour the timeout and will return an empty list of records, so this will act as backoff.
As such retries are not taken into account in the poll loop so you may not need to adjust `max.poll.interval.ms`.
However you still need to make sure that execution time (accumulated time of all calls to external system in a given poll iteration) is less than `max.poll.interval.ms`.

For more details look at `InfiniteRetriesConsumer` class.