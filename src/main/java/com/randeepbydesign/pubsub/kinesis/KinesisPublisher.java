package com.randeepbydesign.pubsub.kinesis;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import com.randeepbydesign.pubsub.JsonPublisher;
import com.randeepbydesign.pubsub.Message;
import com.randeepbydesign.pubsub.Publisher;
import com.randeepbydesign.pubsub.domain.Bottle;
import java.nio.ByteBuffer;

public class KinesisPublisher extends JsonPublisher {

    private final ObjectMapper objectMapper;

    private KinesisProducer publisher;

    private String streamName;

    /**
     *
     * @param publisher
     * @param streamName
     * @param objectMapper Kinesis is much more flexible with what data can be published. We use Jackson mappers
     * to setup JSON data for our purposes
     */
    public KinesisPublisher(KinesisProducer publisher, String streamName, ObjectMapper objectMapper) {
        super(objectMapper);
        this.publisher = publisher;
        this.streamName = streamName;
        this.objectMapper = objectMapper;
    }

    @Override
    public String publish(String subject, String messageBody) {
        try {
            ListenableFuture<UserRecordResult> resp = publisher
                    .addUserRecord(createUserRecord(subject, messageBody));
            /*
            Blocking operation- we could consider using callbacks to process failures asynchronously
            but probably better to catch this critical err at runtime
             */
            return resp.get().getSequenceNumber();
        } catch (Exception e) {
            throw new RuntimeException("Unable to publish message to stream", e);
        }
    }

    private UserRecord createUserRecord(String subject, String messageBody) throws JsonProcessingException {
        UserRecord r = new UserRecord();
        r.setStreamName(this.streamName);
        r.setData(ByteBuffer.wrap(objectMapper.writeValueAsBytes(new Message(subject, null, messageBody))));
        r.setPartitionKey(messageBody);
        return r;
    }

    /**
     *
     * @param args mandatory first argument for the stream name. The region can be specified as an optional second parameter
     */
    public static void main(String[] args) {
        if(args.length<1) {
            System.err.println("Stream Name param required");
            System.exit(1);
        }
        KinesisProducerConfiguration kConfig = new KinesisProducerConfiguration();
        //TODO: configure region, credentials, etc
        if(args.length > 1) {
            kConfig.setRegion(args[1]);
        }
        KinesisProducer kinesisProducer = new KinesisProducer(kConfig);
        Publisher publisher = new KinesisPublisher(kinesisProducer, args[0], new ObjectMapper());
        int counter = 0;

        while (counter < 100) {
            long sleepTime = (long) (Math.random() * 2000l);
            System.out.println("Sleeping for " + sleepTime);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                System.err.println("Thread sleep interrupted: " + e.getLocalizedMessage());
            }
            Bottle b = new Bottle();
            b.setEmpty(false);
            b.setFluidOunces((int) (Math.random() * 100));
            b.setLabel("TestBottle " + counter++);
            b.setPoison(Math.random() < .1d);
            System.out.println("Message published at " + publisher.publishObject(args[0] + " event", b));
        }
    }
}
