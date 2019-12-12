package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SimpleConsumer {

    public static final String PERCENTAGE_FAILURES = "PERCENTAGE_FAILURES";

    public static void main(String[] args) throws Exception {

        SimpleConsumer simpleConsumer = new SimpleConsumer();

        simpleConsumer.start(args, simpleConsumer);
    }

    private final Properties properties = new Properties();
    final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final Random random = new Random();
    private final String TOPIC_NAME = "sample";

    public SimpleConsumer() throws ExecutionException, InterruptedException {
        buildCommonProperties();
        AdminClient adminClient = KafkaAdminClient.create(properties);
        createTopic(adminClient, TOPIC_NAME);

    }

    private void start(String[] args, SimpleConsumer simpleConsumer) throws Exception {
        if (args.length != 1) {
            logger.error("scenario arg is required");
            System.exit(1);
        }


        String retryMode = args[0];
        logger.info("Starting consumer in mode {}", retryMode);

        switch (retryMode) {
            case "NO_RETRY":
                simpleConsumer.noRetryScenario();
                break;
            case "LIMITED_RETRY":
                simpleConsumer.limitedRetryScenario();
                break;
            case "INFINITE_RETRY":
                simpleConsumer.infiniteRetries();
                break;
            default:
                throw new IllegalArgumentException(retryMode + "is not a valid scenario");

        }
    }


    /**
     * Simple consumer without any any retry logic.
     * In case of failure of the remote call you may loose data.
     * Be careful to not have processing time > to 'max.poll.duration.ms' otherwise your instance will be considered as DEAD from consumer group perspective.
     *
     * @throws Exception
     */
    public void noRetryScenario() throws Exception {
        final int PERCENTAGE_FAILURE = getPercentageFailure();

        properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "no-retry");
        // for tests purpose we lower the limit to show importance of those two parameters in such scenarios
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        logger.info("Subscribing to `{}` topic", TOPIC_NAME);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new LogRebalanceListener());


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            logger.info("Fetched {} records", records.count());
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Received offset = {}, partition = {}, key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
                callExternalSystem(record, PERCENTAGE_FAILURE);
            }
            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }
    }

    /**
     * Simple consumer with limited number of retries.
     * After N attemps you may log and or discard the call
     * Be careful to not have processing time (call * retries) > to 'max.poll.duration.ms' otherwise your instance will be considered as DEAD from consumer group perspective.
     *
     * @throws InterruptedException
     */
    public void limitedRetryScenario() throws InterruptedException {

        final int PERCENTAGE_FAILURE = getPercentageFailure(10);
        final int MAX_RETRIES = 3;
        final int BACKOFF = 1000;

        properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "limited-retry");
        // for tests purpose we lower the limit to show importance of those two parameters in such scenarios
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        System.out.println("Subscribing to `" + TOPIC_NAME + "` topic");
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new LogRebalanceListener());


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            System.out.println("Fetched " + records.count() + " records");
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received offset = " + record.offset() + ", partition = " + record.partition() + ", key = " + record.key() + ", value = " + record.value());
                int retries = 0;
                boolean messageDelivered = false;
                while (!messageDelivered && retries <= MAX_RETRIES) {
                    try {
                        callExternalSystem(record, PERCENTAGE_FAILURE);
                        messageDelivered = true;
                    } catch (Exception e) {
                        retries++;
                        System.out.println("Call to external failed " + (MAX_RETRIES - retries) + " retries remaining");
                        TimeUnit.MILLISECONDS.sleep(BACKOFF);
                    }
                }
            }
            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }
    }

    /**
     * Simple consumer with infinite retries.
     * In case of failures, the consumer is paused and offset is set to the first element failing.
     * Next call to the poll(timeout) method will honour the timeout and will return an empty list of records, so this will act as backoff.
     */
    public void infiniteRetries() {

        final int PERCENTAGE_FAILURE = getPercentageFailure(10);
        final int BACKOFF = 1000;

        buildCommonProperties();
        properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "infinite-retry");
        // for tests purpose we lower the limit to show importance of those two parameters in such scenarios
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        System.out.println("Subscribing to `" + TOPIC_NAME + "` topic");
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new LogRebalanceListener());


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(BACKOFF));
            consumer.resume(consumer.assignment());
            System.out.println("Fetched " + records.count() + " records");
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received offset = " + record.offset() + ", partition = " + record.partition() + ", key = " + record.key() + ", value = " + record.value());
                try {
                    callExternalSystem(record, PERCENTAGE_FAILURE);
                } catch (Exception e) {
                    System.out.println(e);
                    consumer.pause(consumer.assignment());
                    consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                    break;
                }

            }
            if (!records.isEmpty() && consumer.paused().isEmpty()) {
                consumer.commitSync();
            }
        }
    }

    private void buildCommonProperties() {
        Map<String, String> systemProperties = System.getenv().entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("KAFKA_"))
                .collect(Collectors.toMap(
                        e -> e.getKey()
                                .replace("KAFKA_", "")
                                .toLowerCase()
                                .replace("_", ".")
                        , e -> e.getValue())
                );

        properties.putAll(systemProperties);
        properties.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    private Integer getPercentageFailure() {
        return getPercentageFailure(0);
    }

    private Integer getPercentageFailure(Integer defaultValue) {
        return System.getenv().containsKey(PERCENTAGE_FAILURES) ? Integer.valueOf(System.getenv(PERCENTAGE_FAILURES)) : defaultValue;
    }

    private void createTopic(AdminClient adminClient, String TOPIC_NAME) throws InterruptedException, ExecutionException {
        if (!adminClient.listTopics().names().get().contains(TOPIC_NAME)) {
            logger.info("Creating topic {}", TOPIC_NAME);
            final NewTopic newTopic = new NewTopic(TOPIC_NAME, 2, (short) 1);
            try {
                CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                topicsCreationResult.all().get();
            } catch (TopicExistsException e) {
                //silent ignore if topic already exists
            }
        }
    }


    private void callExternalSystem(ConsumerRecord<String, String> record, int percentageOfFailures) throws Exception {
        int duration = random.nextInt(1000);
        logger.info("simulating a call to an external system that will take {} for message {}", duration, record.offset());
        TimeUnit.MILLISECONDS.sleep(duration);
        if (random.nextInt(100) < percentageOfFailures) {
            throw new Exception("Call to external system failed");
        }
    }

    private class LogRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            logger.info("Partition revoked : {}", collection);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            logger.info("Partition assigned : {}", collection);
        }
    }
}
