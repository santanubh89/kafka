package com.starter.kafka.twitterproducer;

public class TwitterApplication {
	
	public static void main(String[] args) {
		try {
			new TwitterProducer().createTwitterProducer();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
