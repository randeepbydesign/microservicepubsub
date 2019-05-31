package com.randeepbydesign.pubsub;

import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.randeepbydesign.pubsub.impl.PrintlnProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single class that can be used to run:
 * <ol><li>
 * publisher that writes to a topic
 * </li>
 * <li>
 * consumer that reads from a queue attached to said topic
 * </li>
 * <li>
 * consumer that reads from a dead-letter-queue linked to the previous consumer
 * </li></ol>
 * The program will run as long as it takes for the Publisher to write 100 messages and then will exit. AS a result,
 * some messages may remain.
 */
public class Orchestra {

    private static int counter = 0;

    private static final Logger log = LoggerFactory.getLogger(Orchestra.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println(
                    "Invalid call: program should be invoked with 3 arguments\n\t"
                            + "Topic Name for SNS to write to\n\t"
                            + "A Queue that can be attached to the topic\n\t"
                            + "A Queue that can serve as the dead-letter queue for processing failures");
            System.exit(0);
        }
        final String topicName = args[0];
        final String sqsName = args[1];
        final String dlqName = args[2];

        SnsPublisher publisher = new SnsPublisher(AmazonSNSClientBuilder.defaultClient(), topicName);

        //Verify SQS subscribed to topic
        if (!publisher.verifySqsSubscriptionToTopic(sqsName)) {
            throw new RuntimeException("No valid consumers found subscribing to topic");
        }

        SqsConsumer consumer = new SqsConsumer(AmazonSQSClientBuilder.defaultClient(), sqsName, (message -> {
            String messageBody = message.getMessage().toString();
            if (!messageBody.contains("Poison pill")) {
                System.out
                        .println("simulate successful processing of " + messageBody.replace('\n', ' '));
                return message.getMessageId();
            }
            log.warn("Simulating processing failure at 5% rate for " + messageBody);
            throw new RuntimeException("Poisoned message, cannot process");
        }));
        SqsConsumer dlqConsumer = new SqsConsumer(AmazonSQSClientBuilder.defaultClient(), dlqName,
                new PrintlnProcessor());

        consumer.startPolling();
        dlqConsumer.startPolling();

        //Publish 100 messages and exit
        boolean val = true;
        while (counter < 100) {
            long sleepTime = (long) (Math.random() * 10000l);
            System.out.println("Sleeping for " + sleepTime);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                System.err.println("Thread sleep interrupted: " + e.getLocalizedMessage());
            }
            publisher.publish("MediaPlanEvent",
                    Math.random() > .1d ? "Publishing event " + counter++ : "Poison pill " + counter++);
        }
    }

}
