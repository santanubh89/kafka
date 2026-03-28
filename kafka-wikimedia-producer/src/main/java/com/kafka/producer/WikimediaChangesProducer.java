package com.kafka.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Interceptor;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class WikimediaChangesProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String TOPIC_NAME = "wikimedia.recentchange";

    private static final String CHANGE_STREAM_EVENT_URI = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {
        Properties producerProperties = new Properties();

        /** BASIC PRODUCER CONFIGURATIONS **/
        producerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(INTERCEPTOR_CLASSES_CONFIG, WikimediaChangesProducerInterceptor.class.getName());

        /** SAFE PRODUCER CONFIGURATIONS **/
        producerProperties.setProperty(ACKS_CONFIG, "all");
        producerProperties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.setProperty(RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerProperties.setProperty(DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        producerProperties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        /** HIGH THROUGHPUT PRODUCER CONFIGURATIONS **/
        producerProperties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
        producerProperties.setProperty(LINGER_MS_CONFIG, "20");
        producerProperties.setProperty(BATCH_SIZE_CONFIG, String.valueOf(32 * 1024)); // 32 KB

        producerProperties.setProperty(MAX_BLOCK_MS_CONFIG, "1000");
        producerProperties.setProperty(BUFFER_MEMORY_CONFIG, "33554432");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        EventHandler eventHandler = new WikimediaChangeHandler(producer, TOPIC_NAME);

        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(CHANGE_STREAM_EVENT_URI))
            .client(buildOkHttpClient()).build();
        eventSource.start();

        TimeUnit.SECONDS.sleep(50);
    }

    @NotNull
    private static OkHttpClient buildOkHttpClient() {
        OkHttpClient client = new OkHttpClient.Builder()
            .addInterceptor(new Interceptor() {
                @Override
                public Response intercept(Chain chain) throws IOException {
                    Request original = chain.request();
                    Request requestWithUserAgent = original.newBuilder()
                        .header("User-Agent", "MyKafkaWikimediaProducer/1.0 (your-email@example.com)")
                        .build();
                    return chain.proceed(requestWithUserAgent);
                }
            }).build();
        return client;
    }

}

/*
keytool -importcert -trustcacerts -file "C:\Downloads\_.wikipedia.org.crt" -keystore "C:\Program Files\Eclipse Adoptium\jdk-24.0.0.36-hotspot\lib\security\cacerts" -alias wikimedia
 */