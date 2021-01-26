package com.sample;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.function.BiFunction;

@AllArgsConstructor
@Slf4j
public class RetriableConsumer<K,V,R> {

    @FunctionalInterface
    interface Then<K,V,R>{
        void respond(K key, V value, R result);
    }

    private final BiFunction<K,V, R> processor;

    private final Then<K,V,R> then;

    private RetryPolicy<K, V> policy;

    public void start(){
        while (true) {
            ConsumerRecords<K, V> records = policy.poll();

            for (ConsumerRecord<K, V> record : records) {
                try {
                    R res = processor.apply(record.key(), record.value());
                    log.trace("Processor call suceeded");
                    policy.reset();
                    then.respond(record.key(), record.value(), res);
                } catch (Exception e) {
                    policy.onError(record, e);
                    break;
                }
            }
            if(! records.isEmpty()) policy.commit();
        }
    }

}
