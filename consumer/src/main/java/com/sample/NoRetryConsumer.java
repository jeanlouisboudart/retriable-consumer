package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NoRetryConsumer {

    public static void main(String[] args) throws Exception {
        NoRetryConsumer retriableConsumer = new NoRetryConsumer();
        retriableConsumer.start();
    }

    private static final String TOPIC_NAME = "sample";
    private final Properties properties = KafkaUtils.buildCommonProperties();
    private final Logger logger = LoggerFactory.getLogger(NoRetryConsumer.class);
    private final ExternalService externalService = new ExternalService();

    public NoRetryConsumer() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        KafkaUtils.createTopic(adminClient, TOPIC_NAME);
    }

    /**
     * Simple consumer without any any retry logic.
     * In case of failure of the remote call you may loose data.
     * Be careful to not have processing time > to 'max.poll.duration.ms' otherwise your instance will be considered as DEAD from consumer group perspective.
     *
     * @throws Exception
     */
    public void start() throws Exception {
        properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "no-retry");
        // for tests purpose we lower the limit to show importance of those two parameters in such scenarios
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        logger.info("Subscribing to `{}` topic", TOPIC_NAME);
        consumer.subscribe(Collections.singleton(TOPIC_NAME), new LogRebalanceListener());


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            Instant startTime = Instant.now();
            logger.info("Fetched {} records", records.count());
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Received offset = {}, partition = {}, key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
                try {
                    externalService.callExternalSystem(record);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                KafkaUtils.logDurationSincePoll(startTime);
            }
            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }
    }

}
