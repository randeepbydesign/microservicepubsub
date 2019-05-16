package com.randeepbydesign.pubsub;

import com.amazonaws.services.sqs.model.Message;

/**
 * Generic interface that can be used to process messages. Would probably want to setup something
 * specific for handling the JSON envelopes we are dealing with to extract the actual message and any
 * metadata we are concerned with
 */
public interface MessageProcessor {

    /**
     *
     * @param message
     * @return the receipt handle of messages that are successfuly processed. In the event that the message cannot be
     * processed an exception can be thrown for tracking purposes or just ignored.
     */
    String processMessage(Message message);
}
