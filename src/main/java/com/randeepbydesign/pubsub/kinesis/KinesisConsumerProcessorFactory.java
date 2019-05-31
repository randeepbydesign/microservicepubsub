package com.randeepbydesign.pubsub.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.randeepbydesign.pubsub.MessageProcessor;

public class KinesisConsumerProcessorFactory implements IRecordProcessorFactory {

    private MessageProcessor messageProcessor;

    private MessageProcessorFailureHandler messageProcessorFailureHandler;

    private AcknowledgePolicy acknowledgePolicy;

    private long acknowledgeTimeout;

    public KinesisConsumerProcessorFactory(MessageProcessor messageProcessor,
            MessageProcessorFailureHandler messageProcessorFailureHandler,
            AcknowledgePolicy acknowledgePolicy, long acknowledgeTimeout) {
        this.messageProcessor = messageProcessor;
        this.messageProcessorFailureHandler = messageProcessorFailureHandler;
        this.acknowledgePolicy = acknowledgePolicy;
        this.acknowledgeTimeout = acknowledgeTimeout;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisConsumerProcessor(messageProcessor, messageProcessorFailureHandler, acknowledgePolicy,
                acknowledgeTimeout);
    }

}
