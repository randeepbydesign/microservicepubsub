package com.randeepbydesign.pubsub;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.Topic;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class SnsPublisher {

    private static final Logger log = Logger.getAnonymousLogger();

    final private AmazonSNS publisher;
    final private String topicName;

    private Topic topic = null;

    public SnsPublisher(AmazonSNS publisher, String topicName) {
        this.publisher = publisher;
        this.topicName = topicName;
    }

    public String publish(final String subject, final String messageBody) {
        PublishRequest publishRequest = new PublishRequest();
        publishRequest
                .setMessage(messageBody);
        publishRequest.setSubject(subject);
        publishRequest.setTopicArn(getTopic().getTopicArn());

        /*
        If the connection to the Topic or SNS becomes invalid this becomes a point of failure. Publishing would not
        work and a backup would be needed. For example, storing the messageBody to a datastore for resending when
        the connection does become available.
         */
        System.out.println("Publishing message " + publishRequest.getMessage());
        PublishResult publishResult = publisher.publish(publishRequest);
        System.out.println("Published with id: " + publishResult.getMessageId());
        return publishResult.getMessageId();
    }

    /**
     * Ensure that the given SQS is subscribed to the Topic referred to by this instance
     * @param sqsName
     * @return
     */
    public boolean verifySqsSubscriptionToTopic(final String sqsName) {
        ListSubscriptionsByTopicResult subscriptions = publisher
                .listSubscriptionsByTopic(getTopic().getTopicArn());
        if (subscriptions.getSubscriptions().stream().noneMatch(
                subscription -> subscription.getProtocol().equals("sqs") && subscription.getEndpoint()
                        .endsWith(sqsName))) {
            return false;
        }
        return true;
    }

    private Topic getTopic() {
        if(topic==null) {
            ListTopicsResult res = publisher.listTopics();
            log.info(res.getTopics().stream().map(Topic::getTopicArn).collect(Collectors.joining("\n")));
            List<Topic> topics = res.getTopics().stream()
                    .filter(topic -> topic.getTopicArn().endsWith(":" + topicName))
                    .collect(toList());
            // TODO: Systems with lots of topics will get a paginated response; if not found use next page token

            if (topics.size() == 0) {
                throw new RuntimeException("Topic not found: \"" + topicName + "\"");
            }
            if (topics.size() > 1) {
                throw new RuntimeException("Naming conflict, more than one topic ending with " + topicName);
            }
            topic = topics.get(0);
        }
        return topic;
    }

    public static void main(String[] args) {
        SnsPublisher publisher = new SnsPublisher(AmazonSNSClientBuilder.defaultClient(), args[0]);
        int counter = 0;

        while (counter < 100) {
            long sleepTime = (long) (Math.random() * 2000l);
            System.out.println("Sleeping for " + sleepTime);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                System.err.println("Thread sleep interrupted: " + e.getLocalizedMessage());
            }
            publisher.publish(args[0] + " event",
                    Math.random() > .1d ? "Publishing event " + counter++ : "Poison pill " + counter++);
        }
    }
}
