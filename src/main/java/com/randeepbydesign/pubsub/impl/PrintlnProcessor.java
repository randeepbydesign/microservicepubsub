package com.randeepbydesign.pubsub.impl;

import com.randeepbydesign.pubsub.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple message processor that extracts the subject and message and writes that information to the console.
 */
public class PrintlnProcessor implements MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(PrintlnProcessor.class);

    @Override
    public String processMessage(com.randeepbydesign.pubsub.Message message) {
        log.info("Parsing message:\n\t" + message.getMessage());
        return message.getMessageId();
    }

}
