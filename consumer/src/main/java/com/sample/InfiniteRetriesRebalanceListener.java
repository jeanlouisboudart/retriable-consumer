package com.sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class InfiniteRetriesRebalanceListener implements ConsumerRebalanceListener {
    private final Logger logger = LoggerFactory.getLogger(LogRebalanceListener.class);
    private final Consumer<?,?> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public InfiniteRetriesRebalanceListener(KafkaConsumer<?, ?> consumer, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.consumer = consumer;
        this.offsets = offsets;
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Partition revoked : {}", partitions);
        for (TopicPartition topicPartition : partitions) {
            offsets.remove(topicPartition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Partition assigned : {}", partitions);
        Map<TopicPartition, OffsetAndMetadata> offsetsFromKafka = consumer.committed(new HashSet<>(partitions));
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetsEntry : offsetsFromKafka.entrySet()) {
            //position can be unknown when you first initialize the group, let's be lazy here, when messages will be consumed, the map will be filled.
            if (offsetsEntry.getValue() != null) {
                offsets.put(offsetsEntry.getKey(), offsetsEntry.getValue());
            }
        }
    }
}
