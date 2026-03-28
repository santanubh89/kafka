package com.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ElasticSearchConsumer {

    private static final String ES_CONN_URI = "http://localhost:9200";

    private static final String ES_INDEX_NAME = "wikimedia.recentchange";

    private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient restHighLevelClient = ElasticSearchClient.createESClient(ES_CONN_URI);
        ElasticSearchClient.createWikimediaESIndex(restHighLevelClient, ES_INDEX_NAME);
        log.info("Created OpenSearch client: {}", restHighLevelClient);

        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        kafkaConsumer.subscribe(java.util.Collections.singletonList("wikimedia.recentchange"));

        try (kafkaConsumer; restHighLevelClient) {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(3000));
                log.info("Consumed {} records.", consumerRecords.count());
                BulkRequest bulkRequest = new BulkRequest();
                consumerRecords.forEach(consumerRecord -> {
                    // log.info("Received consumerRecord: key={}, partition={}, offset={}", consumerRecord.key(), consumerRecord.partition(), consumerRecord.offset());
                    // ElasticSearchClient.indexDocument(restHighLevelClient, ES_INDEX_NAME, consumerRecord);
                    IndexRequest indexRequest = new IndexRequest(ES_INDEX_NAME)
                            .source(consumerRecord.value(), XContentType.JSON)
                            // .id(id == null ? record.topic() + "_" + record.partition() + "_" + record.offset() : id)
                            .id(consumerRecord.topic() + "_" + consumerRecord.partition() + "_" + consumerRecord.offset());
                    bulkRequest.add(indexRequest);
                });
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = ElasticSearchClient.bulkRequest(restHighLevelClient, bulkRequest);
                    if (null != bulkResponse && !bulkResponse.hasFailures()) {
                        log.info("Inserted {} records into OpenSearch. Bulk response status: {}", bulkRequest.numberOfActions(), bulkResponse.status());
                    }
                    TimeUnit.SECONDS.sleep(1);

                    kafkaConsumer.commitAsync((offsetData, exception) -> {
                        if (exception == null) {
                            offsetData.keySet().forEach(topicPartition -> {
                                OffsetAndMetadata offsetAndMetadata = offsetData.get(topicPartition);
                                log.info("Committed offsets: Partition: {}, Offset: {}", topicPartition.partition(), offsetAndMetadata.offset());
                            });
                        } else {
                            log.error("Failed to commit offsets: {}", exception.getMessage());
                        }
                    });
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties consumerProperties = new Properties();
        // Basic consumer configurations
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Consumer group and offset reset configurations
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia-es-consumer-group");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Auto commit configurations
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        consumerProperties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(3000));

        // Heartbeat and session timeout configurations
        consumerProperties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(3000));
        consumerProperties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(10000));
        consumerProperties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(300000));
        consumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(100));

        // Fetch configurations to control the amount of data fetched in each poll
        consumerProperties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(100));
        consumerProperties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(100));

        consumerProperties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, WikimediaConsumerInterceptor.class.getName());
        return new KafkaConsumer<>(consumerProperties);
    }


}
