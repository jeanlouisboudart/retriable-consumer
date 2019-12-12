package com.sample;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class LogRebalanceListener implements ConsumerRebalanceListener {

    private final Logger logger = LoggerFactory.getLogger(LogRebalanceListener.class);


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        logger.info("Partition revoked : {}", collection);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        logger.info("Partition assigned : {}", collection);
    }
}
