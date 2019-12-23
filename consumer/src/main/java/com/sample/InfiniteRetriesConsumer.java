package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class InfiniteRetriesConsumer {


    public static void main(String[] args) throws Exception {
        InfiniteRetriesConsumer retrieableConsumer = new InfiniteRetriesConsumer();
        retrieableConsumer.start();
    }

    private static final String TOPIC_NAME = "sample";
    private final Properties properties = KafkaUtils.buildCommonProperties();
    private final Logger logger = LoggerFactory.getLogger(InfiniteRetriesConsumer.class);
    private final ExternalService externalService = new ExternalService();
    private final Map<TopicPartition, OffsetAndMetadata> offsets =  new HashMap<>();

    public InfiniteRetriesConsumer() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        KafkaUtils.createTopic(adminClient, TOPIC_NAME);
    }

    /**
     * Simple consumer with infinite retries.
     * In case of failures, the consumer is paused and offset is set to the first element failing.
     * Next call to the poll(timeout) method will honour the timeout and will return an empty list of records, so this will act as backoff.
     */
    public void start() {

        final int BACKOFF = 1000;

        properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "infinite-retry");
        // for tests purpose we lower the limit to show importance of those two parameters in such scenarios
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        logger.info("Subscribing to `{}` topic", TOPIC_NAME);
        consumer.subscribe(Collections.singleton(TOPIC_NAME), new InfiniteRetriesRebalanceListener(consumer, offsets));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(BACKOFF));
            Instant startTime = Instant.now();
            if (isPaused(consumer)) {
                consumer.resume(consumer.assignment());
            }
            logger.info("Fetched {} records ", records.count());
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Received offset = {}, partition = {}, key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
                try {
                    externalService.callExternalSystem(record);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    consumer.pause(consumer.assignment());
                    rewind(consumer);
                    break;
                }
                KafkaUtils.logDurationSincePoll(startTime);
                updateOffsetsPosition(record);
            }
            if (!records.isEmpty() && !isPaused(consumer)) {
                consumer.commitSync(offsets);
            }
        }
    }

    private boolean isPaused(KafkaConsumer<?,?> consumer) {
        return !consumer.paused().isEmpty();

    }

    private void updateOffsetsPosition(ConsumerRecord<String, String> record) {
        offsets.put(new TopicPartition(record.topic(),record.partition()), new OffsetAndMetadata(record.offset() + 1));
    }

    private void rewind(KafkaConsumer<String, String> consumer) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (entry.getValue() != null) {
                consumer.seek(entry.getKey(), entry.getValue());
            } else {
                logger.warn("Cannot rewind on {} to null offset, this could happen if the consumer group was just created", entry.getKey());
            }

        }
    }
}
