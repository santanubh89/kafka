package com.starter.kafka.twitterproducer;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TwitterUtil {
	
	public static String getTweetId(String message) {
		Map<String, Object> messageMap = null;
		try {
			messageMap = new ObjectMapper().readValue(message, HashMap.class);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return String.valueOf((Long)messageMap.get("id"));
	}

}
