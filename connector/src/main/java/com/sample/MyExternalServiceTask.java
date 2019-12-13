package com.sample;


import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class MyExternalServiceTask extends SinkTask {

    private static final Integer INFINITE_RETRIES = -1;
    private final Logger log = LoggerFactory.getLogger(MyExternalServiceTask.class);

    private final ExternalService externalService = new ExternalService();
    private MyExternalServiceSinkConfig config;

    int remainingRetries;


    @Override
    public void start(Map<String, String> props) {
        log.info("Starting MyExternalService Sink task");
        config = new MyExternalServiceSinkConfig(props);
        remainingRetries = config.maxRetries;

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        try {
            for (SinkRecord record : records) {
                externalService.callExternalSystem(config.url,record);
            }
        } catch (Exception e) {
            log.warn(
                    "Write of {} records failed, remainingRetries={}",
                    records.size(),
                    remainingRetries,
                    e
            );
            if (config.retryBackoffMs == INFINITE_RETRIES || remainingRetries > 0) {
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(e);
            } else {
               throw new ConnectException(e);
            }
        }
        remainingRetries = config.maxRetries;
    }

    @Override
    public void stop() {

    }

    @Override
    public String version() {
        return new MyExternalServiceConnector().version();
    }
}
