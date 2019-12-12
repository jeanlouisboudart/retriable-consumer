package com.sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ExternalService {
    private static final String PERCENTAGE_FAILURES = "PERCENTAGE_FAILURES";
    private final Logger logger = LoggerFactory.getLogger(LimitedRetriesConsumer.class);
    private final Random random = new Random();

    private final int percentageOfFailures;

    public ExternalService() {
        // Default to 10 % of failures
        percentageOfFailures = System.getenv().containsKey(PERCENTAGE_FAILURES) ? Integer.valueOf(System.getenv(PERCENTAGE_FAILURES)) : 10;
        logger.info("External system call will have {}% chances of failures", percentageOfFailures);
    }

    public void callExternalSystem(ConsumerRecord<String, String> record) throws Exception {
        int duration = random.nextInt(1000);
        logger.info("simulating a call to an external system that will take {} for message {}", duration, record.offset());
        TimeUnit.MILLISECONDS.sleep(duration);
        if (random.nextInt(100) < percentageOfFailures) {
            throw new Exception("Call to external system failed");
        }
    }
}
