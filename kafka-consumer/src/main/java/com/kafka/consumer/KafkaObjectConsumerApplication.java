package com.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaObjectConsumerApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaObjectConsumerApplication.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "application_log";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Starting Kafka Consumer");

        // Create consumer properties
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "log_processor");

        // Static group instance ID for testing rebalance behavior. In production, this should be unique per consumer instance.
        consumerProperties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance_1");
        consumerProperties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProperties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");

        KafkaConsumer<String, EmployeeData> consumer = new KafkaConsumer<>(consumerProperties);

        Thread currentThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received. Closing Kafka Consumer.");
            consumer.wakeup();
            try {
                currentThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            while (true) {
                ConsumerRecords<String, EmployeeData> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, EmployeeData> record : records) {
                    processConsumedRecord(record);
                }
            }
        } catch (WakeupException e) {
            logger.info("WakeupException: Consumer is shutting down.");
        } catch (Exception e) {
            logger.error("Error occurred while consuming records", e);
        } finally {
            consumer.close();
            logger.info("Kafka Consumer gracefully shutdown.");
        }
    }

    private static void processConsumedRecord(ConsumerRecord<String, EmployeeData> record) {
        StringBuilder log = new StringBuilder("Message Consumed::");
        log.append("Key:").append(record.key());
        log.append(";Value:").append(record.value());
        Headers headers = record.headers();
        headers.forEach(header -> log.append(";").append(header.key()).append(":").append(new String(header.value())));
        log.append(";Partition:").append(record.partition());
        log.append(";Offset:").append(record.offset());
        log.append(";Timestamp:").append(record.timestamp());
        logger.info(log.toString());
    }

}
