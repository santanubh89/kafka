package com.kafka.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;

@Slf4j
public class WikimediaChangeHandler implements EventHandler {

    private final String topic;

    private final KafkaProducer<String, String> kafkaProducer;

    public WikimediaChangeHandler(final KafkaProducer<String, String> kafkaProducer, final String topic) {
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info("Wikimedia change event received: {} with body: {}", event, messageEvent.getData());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, UUID.randomUUID().toString(), messageEvent.getData());
        kafkaProducer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                // log.info("Event published. Topic: {}, Partition: {}, Offset: {}",
                   //     recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            } else {
                // log.error("Error publishing event: ", exception);
            }
        });
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Wikimedia change event error: ", throwable);
    }
}
