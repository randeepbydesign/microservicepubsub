# Microservice PubSub with AWS SNS & SQS
Using SNS/SQS to implement a simple and resilient Pub-Sub system that can be used for Microservice messaging

## What's the big deal?
There are a lot of asynchronous messaging solutions out there. RabbitMQ, Kafka, JMS, just to name a few. If you're already
in the cloud with AWS, however, you might want to consider a simple solution that scales well for low to medium throughput
and can handle messaging failures resiliently. This solution has the following features:

- Publishers can easily write to one-to-many topics as needed.
- They do not need to consider who is reading their published messages
  - That being said, AWS security features can be used here to implement security restrictions
- Consumers can subscribe to topics through a variety of mediums (email, SMS, Lambda function, etc)
  - In this example, the consumer attaches an SQS to the topic.
  - SQS has a read/acknowledge system to limit the same message from being read more than once (ALO 
  processing with strong tendencies towards EO)
  - AWS provides a built-in system for error handling in the form of a dead-letter queue. Messages that 
  cannot be acknowledged (deleted) will be moved to this backup queue.

## How does it work?
Simple Notification Service and Simple Queue Service (heretofore abbreviated to SNQS) allows for 
topic based publishing with multiple consumers in an AWS managed environment. To setup an SNQS, a lot of
setup and configuration needs to be done, preferably in the console (although most of these tasks are exposed
via the AWS API):
1. Create an SQS that can function as a dead-letter queue. It should be configured so that messages remain
for enough days for solutions to be addressed in the event of processing failure. 
    - Alerts should also be setup to notify support persons in the event of failure
2. Create an SQS that will subscribe to an SNS topic.
   - It should be configured to use the dead-letter queue setup up in the prior step using an appropriate
   redrive policy.
2. Create an SNS topic with a unique name
   - Define a subscriber to the topic in the form of the queue created in the prior step
   
Once this setup is done you can run the code provided here. The `Orchestra` class will run all 3 services
(1 SNS publisher, 1 SQS to consume messages, 1 SQS to receive processing failures.)

Alternatively, you can the Publisher and Consumers via their respective `main` methods to monitor the threads
separately. To run the SqsConsumer as the primary consumer pass the queue name and "poison" as a 2nd argument. Poison
will send the random messages containing poison pill to the dead letter queue.

## What are the benefits of this solution compared to others?
- Fully managed by AWS
- Fairly resilient. Points-of-failure are limited in scope and size
- Simple: compared to other solutions there is not much learning curve and the tech is simple. (The pipes are dumb).
- AWS manages read-once transactional logic so users don't have to.
- Consumer errors are managed through configuration-over-code using Redrive policies and Dead Letter queues.
- Consumers receive message processors as a parameter so dependency injection can be used to configure handling.
  - Dead letter queues can be configured using migration-style processors to eliminate unwanted messages or route them back to the original queue, etc.

## What are some of the drawbacks?
- There are still _some_ points of failure (I have documented these in the code with comments), though that will
be the case with all messaging solutions.
- Vendor lock-in
- For local development and testing there is a dependency on AWS. This can be mitigated with use of a 
library like LocalStack