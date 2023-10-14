package com.sample;

import com.sample.RetryPolicy.LimitedRetriesPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class Main {
    private static final String TOPIC_NAME = "sample";

    private final ExternalService externalService = new ExternalService();
    private final Properties properties = KafkaUtils.buildCommonProperties();

    public static void main(String[] args) {
        new Main().start();

    }

    public void start() {
        properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "limited-retry2");
        // for tests purpose we lower the limit to show importance of those two parameters in such scenarios
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        log.info("Subscribing to `{}` topic", TOPIC_NAME);
        consumer.subscribe(Collections.singleton(TOPIC_NAME), new LogRebalanceListener());

        LimitedRetriesPolicy<String, String> policy = new LimitedRetriesPolicy<>(
                2,
                consumer,
                Duration.ofSeconds(1),
                d -> d,
                (k, v) -> log.error("Unable to process record ({},{})", k, v)
        );

        new RetriableConsumer<>(
                (k,v) -> externalService.callExternalSystem(),
                (k,v,r) -> log.info("Suceeded"),
                policy
        ).start();
    }
}
