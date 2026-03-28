package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaObjectProducerApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaObjectProducerApplication.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "application_log";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Starting Kafka Producer");

        EmployeeData employeeData = new EmployeeData("emp1", "John Doe", Department.IT, 75000.0, new Address("New York", "NY", "USA"));
        logger.info("Employee Data: {}", employeeData);

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        KafkaProducer<String, EmployeeData> producer = new KafkaProducer<>(producerProperties);

        ProducerRecord<String, EmployeeData> record = new ProducerRecord<>(TOPIC_NAME, employeeData.id(), employeeData);
        record.headers().add(new RecordHeader("app-name", "java-producer".getBytes()));
        producer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                printRecordMetadata(recordMetadata);
            } else {
                logger.error("Error while producing message: ", exception);
            }
        });
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
