package com.randeepbydesign.pubsub.impl;

import com.randeepbydesign.pubsub.Message;
import com.randeepbydesign.pubsub.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message processor that looks for the phrase 'Poison pill' and, if found, throws an exception
 */
public class PoisonPillMessageProcessor implements MessageProcessor {

    Logger log = LoggerFactory.getLogger(PoisonPillMessageProcessor.class);

    @Override
    public String processMessage(Message message) {
        final String messageBody = message.getMessage().toString();
        if (!messageBody.contains("Poison pill")) {
            log.info("simulate successful processing of " + messageBody.replace('\n', ' '));
            return message.getMessageId();
        }
        log.warn("Simulating processing failure at 5% rate for " + messageBody);
        throw new RuntimeException("Poisoned message, cannot process");
    }

}
