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
import java.util.concurrent.TimeUnit;

public class LimitedRetriesConsumer {



    public static void main(String[] args) throws Exception {
        LimitedRetriesConsumer retriableConsumer = new LimitedRetriesConsumer();
        retriableConsumer.start();
    }

    private static final String TOPIC_NAME = "sample";
    public static final String MAX_RETRIES = "MAX_RETRIES";
    private final Properties properties = KafkaUtils.buildCommonProperties();
    private final Logger logger = LoggerFactory.getLogger(LimitedRetriesConsumer.class);
    private final ExternalService externalService = new ExternalService();
    private final int maxRetries;


    public LimitedRetriesConsumer() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaAdminClient.create(properties);
        KafkaUtils.createTopic(adminClient, TOPIC_NAME);
        maxRetries = System.getenv().containsKey(MAX_RETRIES) ? Integer.valueOf(System.getenv(MAX_RETRIES)) : 10;
    }

    /**
     * Simple consumer with limited number of retries.
     * After N attemps you may log and or discard the call
     * Be careful to not have processing time (call * retries) > to 'max.poll.duration.ms' otherwise your instance will be considered as DEAD from consumer group perspective.
     *
     * @throws InterruptedException
     */
    public void start() throws InterruptedException {

        final int BACKOFF = 1000;

        properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "limited-retry");
        // for tests purpose we lower the limit to show importance of those two parameters in such scenarios
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000"); // default to 5min
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20"); // default 500

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        logger.info("Subscribing to `{}` topic", TOPIC_NAME);
        consumer.subscribe(Collections.singleton(TOPIC_NAME), new LogRebalanceListener());


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            Instant startTime = Instant.now();
            logger.info("Fetched {} records ", records.count());
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Received offset = {}, partition = {}, key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
                int retries = 0;
                boolean messageDelivered = false;
                while (!messageDelivered && retries <= maxRetries) {
                    try {
                        externalService.callExternalSystem(record);
                        messageDelivered = true;
                    } catch (Exception e) {
                        retries++;
                        logger.warn("Call to external failed " + (maxRetries - retries) + " retries remaining");
                        TimeUnit.MILLISECONDS.sleep(BACKOFF);
                    }
                    KafkaUtils.logDurationSincePoll(startTime);
                }
            }
            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }
    }
}
