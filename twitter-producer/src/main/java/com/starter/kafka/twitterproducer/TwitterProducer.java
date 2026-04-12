package com.starter.kafka.twitterproducer;

import static com.starter.kafka.twitterproducer.TwitterProducerConstants.ACCESS_SECRET;
import static com.starter.kafka.twitterproducer.TwitterProducerConstants.ACCESS_TOKEN;
import static com.starter.kafka.twitterproducer.TwitterProducerConstants.CONSUMER_KEY;
import static com.starter.kafka.twitterproducer.TwitterProducerConstants.CONSUMER_SECRET;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;;

public class TwitterProducer {

	public void createTwitterProducer() throws InterruptedException {
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		Client twitterClient = createTwitterClient(msgQueue);
		KafkaProducer<String, String> kafkaProducer = kafkaProducer();
		while (!twitterClient.isDone()) {
			String message = msgQueue.poll(5, TimeUnit.SECONDS);
			if (message != null) {
				String tweetId = TwitterUtil.getTweetId(message);
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TwitterProducerConstants.TOPIC, tweetId, message);
				kafkaProducer.send(producerRecord, (metadata, exception) -> {
					if (null != exception) {
						System.out.println("Error sending message: " + tweetId + ", error is: " + exception.getMessage());
					} else {
						String format = "Message Key: %s\t Topic: %s\t Partition: %s\t Offset: %s";
						String info = String.format(format, tweetId, metadata.topic(), metadata.partition(), metadata.offset());
						System.out.println(info);
					}
				});
				System.out.println("Message: " + message);
			}
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Shutting Down Application...");
			twitterClient.stop();
			kafkaProducer.close();
		}));
	}

	private Client createTwitterClient(BlockingQueue<String> msgQueue) {
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		hosebirdEndpoint.trackTerms(TwitterProducerConstants.FOLLOW_TERMS);
		Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET);
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		Client hosebirdClient = builder.build();
		hosebirdClient.connect();
		return hosebirdClient;
	}
	
	private KafkaProducer<String, String> kafkaProducer() {
		Properties properties = new Properties();
		properties.putAll(kafkaProperties());
		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}
	
	private Map<String, String> kafkaProperties() {
		Map<String, String> kafkaProperties = new HashMap<String, String>();
		kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TwitterProducerConstants.BOOTSTRAP_SERVER);
		kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Safe producer
		kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "all");
		kafkaProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		kafkaProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		// High throughput producer
		kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		kafkaProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64*1024));
		kafkaProperties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
		
		kafkaProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
		kafkaProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Integer.toString(32*1024*1024));
		
		return kafkaProperties;
	}

}
