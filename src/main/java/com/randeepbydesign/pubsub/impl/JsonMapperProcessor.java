package com.randeepbydesign.pubsub.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.randeepbydesign.pubsub.Message;
import com.randeepbydesign.pubsub.MessageProcessor;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JsonMapperProcessor<T> implements MessageProcessor {

    Logger log = LoggerFactory.getLogger(JsonMapperProcessor.class);

    private ObjectMapper objectMapper;
    private Class<T> clazz;

    public JsonMapperProcessor(ObjectMapper objectMapper, Class<T> clazz) {
        this.objectMapper = objectMapper;
        this.clazz = clazz;
    }

    @Override
    public String processMessage(Message message) {
        T ret;
        try {
            ret = objectMapper.readValue(message.getMessage(), clazz);
        } catch (IOException e) {
            log.error("Unable to deserialize message", e);
            throw new RuntimeException(e);
        }
        processMessageObject(ret);
        return message.getMessageId();
    }

    public abstract Object processMessageObject(T objectInstance);
}
