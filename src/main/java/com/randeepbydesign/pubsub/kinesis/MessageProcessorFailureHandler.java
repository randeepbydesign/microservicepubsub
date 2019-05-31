package com.randeepbydesign.pubsub.kinesis;

import com.randeepbydesign.pubsub.Message;

/**
 * Can be defined to setup a workflow in the event a messaqge can't be processed
 */
public interface MessageProcessorFailureHandler {

    /**
     *
     * @param message
     */
    void handleFailure(Message message);
}
