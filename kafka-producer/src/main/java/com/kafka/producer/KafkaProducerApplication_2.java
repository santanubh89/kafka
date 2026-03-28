package com.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerApplication_2 {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerApplication_2.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "application_log";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Create producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "20");
        producerProperties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int i = 0; i < 300; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "id_"+i, "Message with ID: " + i);
            producer.send(record, (recordMetadata, exception) -> {
                if (exception == null) {
                    printRecordMetadata(recordMetadata);
                } else {
                    logger.error("Error while producing message: ", exception);
                }
            });
        }

        producer.flush();
        producer.close();
        logger.info("Kafka Producer finished sending messages");
    }

    private static void printRecordMetadata(RecordMetadata recordMetadata) {
        StringBuilder log = new StringBuilder("Message Published::");
        log.append("Topic:").append(recordMetadata.topic());
        log.append(";Partition:").append(recordMetadata.partition());
        log.append(";Offset:").append(recordMetadata.offset());
        log.append(";Timestamp:").append(recordMetadata.timestamp());
        logger.info(log.toString());
    }

}
