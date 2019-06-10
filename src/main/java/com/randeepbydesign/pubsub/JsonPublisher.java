package com.randeepbydesign.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Publisher implementation that can be used to serialize objects to JSON before Publishing
 */
public abstract class JsonPublisher implements Publisher {

    private ObjectMapper objectMapper;

    public JsonPublisher(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String publishObject(final String subject, Object messageObject) {
        try {
            return publish(subject, objectMapper.writeValueAsString(messageObject));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to serialize object to JSON", e);
        }
    }

}
