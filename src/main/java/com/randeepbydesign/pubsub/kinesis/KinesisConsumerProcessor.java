package com.randeepbydesign.pubsub.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.randeepbydesign.pubsub.Message;
import com.randeepbydesign.pubsub.MessageProcessor;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class KinesisConsumerProcessor implements IRecordProcessor {

    Logger log = LoggerFactory.getLogger(KinesisConsumerProcessor.class);

    private MessageProcessor messageProcessor;

    private MessageProcessorFailureHandler failoverHandler;

    private AcknowledgePolicy acknowledgePolicy;

    private long acknowledgeTimeout = 0;

    private LocalDateTime startTime;

    /**
     * Convert a Kinesis Record to the Generic Message format
     */
    private Function<Record, Message> toMessage = r -> new Message(r.getSequenceNumber(),
            StandardCharsets.UTF_8.decode(r.getData()).toString());

    /**
     * @param acknowledgeTimeout in millis; the time to wait between acknowledgements if the acknowledgePolicy is set to
     * Periodically
     */
    public KinesisConsumerProcessor(MessageProcessor messageProcessor, MessageProcessorFailureHandler failoverHandler,
            AcknowledgePolicy acknowledgePolicy, final long acknowledgeTimeout) {
        this.messageProcessor = messageProcessor;
        this.failoverHandler = failoverHandler;
        this.acknowledgePolicy = acknowledgePolicy;
        this.acknowledgeTimeout = acknowledgeTimeout;
        startTime = LocalDateTime.now();
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        log.info(initializationInput.getShardId() + " shard processing from " + initializationInput
                .getExtendedSequenceNumber());
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        List<Message> successes = new ArrayList<>();
        List<Message> failures = new ArrayList<>();
        for (Message message : processRecordsInput.getRecords()
                .stream()
                .map(toMessage).collect(toList())) {
            try {
                messageProcessor.processMessage(message);
                successes.add(message);
            } catch (Exception e) {
                log.info("Error processing message " + message.getMessageId() + ": " + e.getLocalizedMessage());
                failoverHandler.handleFailure(message);
                failures.add(message);
            }
            if (AcknowledgePolicy.EVERY_MESSAGE.equals(acknowledgePolicy)) {
                try {
                    processRecordsInput.getCheckpointer().checkpoint(message.getMessageId());
                } catch (Exception e) {
                    log.info("Unable to checkpoint: " + e.getLocalizedMessage());
                }
            }
        }
        acknowledge(successes, failures, processRecordsInput.getCheckpointer());
    }

    private void acknowledge(List<Message> successes, List<Message> failures,
            IRecordProcessorCheckpointer checkpointer) {
        try {
            switch (acknowledgePolicy) {
                case EVERY_BATCH:
                    log.info("Checkpointing at end of batch");
                    checkpointer.checkpoint();
                    break;
                case PERIODICALLY:
                    if (Duration.between(startTime, LocalDateTime.now()).toMillis() > acknowledgeTimeout) {
                        log.info("Executing period checkpoint and rese4tting clock");
                        checkpointer.checkpoint();
                        startTime = LocalDateTime.now();
                    }
                    break;
            }
        } catch (Exception e) {
            log.info("Unable to checkpoint: " + e.getLocalizedMessage());
            e.printStackTrace();
        }

    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        log.info("Shutting down kinesis consumer");
        try {
            shutdownInput.getCheckpointer().checkpoint();
        } catch (ThrottlingException e) {
            log.info("Unable to checkpoint on shutdown due to throttled call. This would be retryable");
        } catch (Exception e) {
            log.info("Unable to checkpoint on shutdown: " + e.getLocalizedMessage());
        }
    }

}
