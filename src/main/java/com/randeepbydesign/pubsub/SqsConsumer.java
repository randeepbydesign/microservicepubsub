package com.randeepbydesign.pubsub;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.randeepbydesign.pubsub.impl.PoisonPillMessageProcessor;
import com.randeepbydesign.pubsub.impl.PrintlnProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

/**
 * Setup a polling consumer that can run in a separate thread
 */
public class SqsConsumer implements MessageConsumer {

    private Logger log = LoggerFactory.getLogger(SqsConsumer.class);

    private final MessageProcessor messageProcessor;

    private boolean stopRunning;

    private final AmazonSQS consumer;

    private final String sqsName;

    Thread t;

    private String sqsUrl;

    /**
     *
     */
    public SqsConsumer(AmazonSQS consumer, String sqsName, MessageProcessor messageProcessor) {
        this.consumer = consumer;
        this.sqsName = sqsName;
        this.messageProcessor = messageProcessor;
        this.sqsUrl = null;
    }

    /**
     * Kicks off a thread to begin listening for messages from the configured queue which will be processed according to
     * the input messageProcessor
     */
    @Override
    public void startPolling() {
        if (t != null && t.isAlive()) {
            log.info("Consumer appears to already be running");
            return;
        }
        stopRunning = false;
        t = new Thread(() -> {
            while (!stopRunning) {
                pollAndConsume(this.getSqsUrl(), this.getReceiveMessageRequest(), messageProcessor);
            }
            log.info("Terminating execution");
        });
        t.start();
    }

    /**
     * If the queue thread is running, notifies it to terminate
     */
    @Override
    public void stopPolling() {
        if (t == null) {
            return;
        }
        this.stopRunning = true;
        t.interrupt();
    }

    private ReceiveMessageRequest getReceiveMessageRequest() {
        ReceiveMessageRequest ret = new ReceiveMessageRequest();
        //If no messages are found, the process will hang out for the specified amount of time for messages to arrive.
        ret.setWaitTimeSeconds(8);
        ret.setQueueUrl(this.getSqsUrl());
        ret.setMaxNumberOfMessages(1);
        return ret;
    }

    private String getSqsUrl() {
        if (sqsUrl == null) {
            log.info("Listing Queues");
            sqsUrl = consumer.listQueues().getQueueUrls().stream()
                    .filter(url -> url.endsWith(sqsName)).findAny()
                    .orElseThrow(() -> new RuntimeException("Unable to locate SQS " + sqsName));
        }
        return sqsUrl;
    }

    /**
     * Ping the server to get messages in the queue; removes successfully processed messages
     */
    private void pollAndConsume(final String sqsUrl, ReceiveMessageRequest request,
            MessageProcessor messageProcesser) {
        final List<String> successfullyProcessedReceipts = new ArrayList<>();
        /**
         * Making this call will set the flag, so-to-speak, on the messages that are returned so that
         * AWS does not deliver them to another consumer. This lock will be valid for the duration of
         * the visibility timeout flag. Setting the value to a high amount means that, if processing fails,
         * a reattempt cannot happen until the time expires.
         *
         * Setting it too low, however, means that if the process of consuming the message takes longer than expected
         * it may be processed multiple times.
         */
        ReceiveMessageResult res = consumer.receiveMessage(request);
        log.info("Recieved " + res.getMessages().size() + " for processing from " + sqsUrl);
        for (Message message : res.getMessages()) {
            try {
                final String receipt = messageProcesser.processMessage(convertMessage(message));
                if (receipt != null) {
                    successfullyProcessedReceipts.add(receipt);
                }
            } catch (Exception e) {
                log.info("Unable to process message: " + e.getLocalizedMessage());
            }
        }

        /*
        Point of failure: if the program unexpectedly terminates here, the messages would have been processed but not
        removed and thus, would be processed again. Some Transactional magic could be used to mitigate this, but its
        best to try and make processing idempotent.
         */
        if (successfullyProcessedReceipts.size() > 0) {
            log.info("Acknowledging " + successfullyProcessedReceipts.size() + " for removal");
            DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
            deleteMessageBatchRequest.setQueueUrl(sqsUrl);
            deleteMessageBatchRequest.setEntries(successfullyProcessedReceipts.stream()
                    .map(receipt -> {
                        DeleteMessageBatchRequestEntry req = new DeleteMessageBatchRequestEntry();
                        req.setReceiptHandle(receipt);
                        req.setId(UUID.randomUUID().toString());
                        return req;
                    }).collect(toList()));
            consumer.deleteMessageBatch(deleteMessageBatchRequest);
        }
    }

    private static final Pattern DATA_EXTRACTOR = Pattern
            .compile(".*\\\"Subject\\\"\\s*:\\s*\\\"([^\"]+)\\\".*\\\"Message\\\"\\s*:\\s*\\\"([^\"]+)\\\".+$",
                    Pattern.DOTALL);

    private com.randeepbydesign.pubsub.Message convertMessage(Message message) {
        Matcher m = DATA_EXTRACTOR.matcher(message.getBody());
        if (!m.matches()) {
            throw new RuntimeException("Unable to parse message");
        }
        return new com.randeepbydesign.pubsub.Message(message.getReceiptHandle(), m.group(1), m.group(2));
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            System.err.println("1 argument required: sqsName");
            return;
        }
        MessageProcessor processor = null;
        if (args.length > 1) {
            String requestedProcessor = args[1].toLowerCase();
            switch (requestedProcessor) {
                case "println":
                    processor = new PrintlnProcessor();
                    break;
                case "poison":
                    processor = new PoisonPillMessageProcessor();
                    break;
                //Would be good to create a "copy" processor that can move messages from DeadLetter Queue to the normal queue
                default:
                    throw new RuntimeException("Invalid processor requested");
            }
        } else {
            processor = new PrintlnProcessor();
        }

        SqsConsumer consumer = new SqsConsumer(AmazonSQSClientBuilder.defaultClient(), args[0], processor);
        consumer.startPolling();
        Thread.sleep(300000l);
        consumer.stopPolling();
    }

}
