package com.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;

@Slf4j
public class JsonDeserializer implements Deserializer<Object> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Object deserialize(String s, byte[] bytes) {
        return (EmployeeData) mapper.readValue(bytes, EmployeeData.class);
    }

}
