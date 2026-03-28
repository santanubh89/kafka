package com.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;

@Slf4j
public class WikimediaConsumerInterceptor implements ConsumerInterceptor<String, String> {
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        log.info("Number of messages consumed: {}", consumerRecords.count());
        return consumerRecords;
    }

    @Override
    public void close() {

    }

    @Override
    public void onCommit(Map map) {
        log.info("Committed offsets: {}", map);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
