package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SimpleProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.start();
    }



    public SimpleProducer() throws ExecutionException, InterruptedException {
        buildCommonProperties();
        AdminClient adminClient = KafkaAdminClient.create(properties);
        createTopic(adminClient, TOPIC_NAME);
    }

    private final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final Properties properties = new Properties();
    private final String TOPIC_NAME = "sample";
    Random random = new Random();

    private void start() throws InterruptedException {
        logger.info("Sending data to `{}` topic", TOPIC_NAME);
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            int[] values = new int[]{0, 0};
            while (true) {
                int key = random.nextInt(2);
                //ProducerRecord<String, String> record = new ProducerRecord<>("sample", key, "Key " + key, "Value " + values[key]);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Key " + key, "Value " + values[key]);
                logger.info("Sending Key = {}, Value = {}", record.key(), record.value());
                producer.send(record);
                values[key]++;
                TimeUnit.SECONDS.sleep(1);
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
        properties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
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

}
