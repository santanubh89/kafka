package com.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.UUID;

@Slf4j
public class WikimediaChangesProducerInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        producerRecord.headers().add("event", "wikimedia-change".getBytes());
        producerRecord.headers().add("event-id", UUID.randomUUID().toString().getBytes());
        producerRecord.headers().add("origin", "wikimedia-changes-producer".getBytes());
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (null == e) {
            log.info("Event Published::Topic: {}; Partition: {}; Offset: {}",
                     recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        } else {
            log.error("Error publishing event: ", e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        log.info("Producer interceptor configured with properties: {}", map);
    }
}
