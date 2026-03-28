package com.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

@Slf4j
public class JsonSerializer implements Serializer<Object> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Object obj) {
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(obj);
    }

}
