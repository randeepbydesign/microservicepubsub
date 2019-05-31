package com.randeepbydesign.pubsub;

/**
 * Generic contract for a consumer of messages to follow
 */
public interface MessageConsumer {

    /**
     * Start an ongoing process to poll the message publisher source for messages to process. Generally this is in the
     * form of a thread that pings a server at intervals for new messages
     */
    void startPolling();

    /**
     * Stop the polling process for shutdown or other reasons
     */
    void stopPolling();
}
