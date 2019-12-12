package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaUtils {
    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    public static Properties buildCommonProperties() {
        Properties properties = new Properties();
        Map<String, String> systemProperties = System.getenv().entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(KAFKA_ENV_PREFIX))
                .collect(Collectors.toMap(
                        e -> e.getKey()
                                .replace(KAFKA_ENV_PREFIX, "")
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
        return properties;
    }


    public static void createTopic(AdminClient adminClient, String TOPIC_NAME) throws InterruptedException, ExecutionException {
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

    /*
    This method is here just to help to understand how long operations such as call to external operations could take time
     */
    public static void logDurationSincePoll(Instant startTime) {
        String durationHumanReadable = Duration.between(startTime, Instant.now())
                .toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();

        logger.info("Duration since poll {}", durationHumanReadable);


    }
}
