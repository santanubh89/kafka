package com.starter.kafka.twitterproducer;

import java.util.ArrayList;

import com.google.common.collect.Lists;

public class TwitterProducerConstants {
	
	public static final String CONSUMER_KEY = "***";
	public static final String CONSUMER_SECRET = "***";
	public static final String ACCESS_TOKEN = "****-***";
	public static final String ACCESS_SECRET = "***";
	
	public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	public static final String TOPIC = "tweets";
	
	public static final ArrayList<String> FOLLOW_TERMS = Lists.newArrayList("India");
	
}
