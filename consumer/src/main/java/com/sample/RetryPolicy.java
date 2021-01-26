package com.sample;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.function.Function;

public interface RetryPolicy<K,V>{
    ConsumerRecords<K,V> poll();

    void onError(ConsumerRecord<K, V> record, Exception e);

    void reset();

    void commit();

    @FunctionalInterface
    interface ErrorManager<K,V>{
        void manage(K key, V value);
    }

    @Slf4j
    class LimitedRetriesPolicy<K,V> implements RetryPolicy<K,V> {

        private final int maxTries;
        private final KafkaConsumer<K, V> consumer;
        private final Duration initialPollDuration;
        private int tries = 0;
        private Duration pollDuration;
        private final Function<Duration, Duration> backOffPolicy;
        private ErrorManager<K, V> errorManager;

        public LimitedRetriesPolicy(int maxTries,
                                    KafkaConsumer<K, V> consumer,
                                    Duration initialPollDuration,
                                    Function<Duration, Duration> backOffPolicy,
                                    ErrorManager<K,V> errorManager) {
            this.maxTries = maxTries;
            this.consumer = consumer;
            this.initialPollDuration = initialPollDuration;
            this.pollDuration = initialPollDuration;
            this.backOffPolicy = backOffPolicy;
            this.errorManager = errorManager;
        }

        @Override
        public ConsumerRecords<K, V> poll() {
            if (log.isTraceEnabled()) log.trace("Polling for {} ms", pollDuration.toMillis());
            ConsumerRecords<K, V> records = consumer.poll(pollDuration);
            consumer.resume(consumer.assignment());
            return records;
        }

        @Override
        public void onError(ConsumerRecord<K, V> record, Exception e) {
            tries++;
            log.warn("Processing failed ({} tries)", tries, e);
            this.pollDuration = backOffPolicy.apply(pollDuration);
            if (tries >= maxTries) reject(record); else pause(record);
        }

        private void reject(ConsumerRecord<K, V> record) {
            log.error("{} tries where run for {}, giving up ...", tries, record.key());
            errorManager.manage(record.key(), record.value());
        }

        private void pause(ConsumerRecord<K, V> record) {
            log.trace("Pausing for retrying");
            consumer.pause(consumer.assignment());
            consumer.seek(
                    new TopicPartition(
                            record.topic(),
                            record.partition()
                    ),
                    record.offset()
            );
        }

        @Override
        public void reset() {
            log.trace("Reseting policy");
            pollDuration = initialPollDuration;
            tries = 0;
        }

        @Override
        public void commit() {
            if (consumer.paused().isEmpty())
                consumer.commitAsync((offsets, exception) -> {
                    if (exception == null) return;
                    log.error("Commit failed, exiting", exception);
                    System.exit(-1);
                });
        }
    }

}
