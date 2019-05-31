package com.randeepbydesign.pubsub.kinesis;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.randeepbydesign.pubsub.MessageConsumer;
import com.randeepbydesign.pubsub.impl.PoisonPillMessageProcessor;
import com.randeepbydesign.pubsub.kinesis.impl.SqsFailureHandler;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisConsumer implements MessageConsumer {

    private Logger log = LoggerFactory.getLogger(KinesisConsumer.class);

    private IRecordProcessorFactory rpf;

    private KinesisClientLibConfiguration kinesisConfiguration;

    private Worker kinesisWorker;

    public KinesisConsumer(IRecordProcessorFactory rpf,
            KinesisClientLibConfiguration kinesisConfiguration) {
        this.rpf = rpf;
        this.kinesisConfiguration = kinesisConfiguration;
    }

    @Override
    public void startPolling() {
        log.info("Starting Kinesis worker");
        kinesisWorker = new Worker.Builder()
                .recordProcessorFactory(rpf)
                .config(kinesisConfiguration)
                .build();
        kinesisWorker.run();
    }

    @Override
    public void stopPolling() {
        log.info("Shutting down Kinesis worker");
        kinesisWorker.shutdown();
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Consumer application name and Stream Name params required");
            System.exit(1);
        }
        KinesisConsumerProcessorFactory rpf = new KinesisConsumerProcessorFactory(new PoisonPillMessageProcessor(),
                null, 
                AcknowledgePolicy.EVERY_BATCH, 5000l);
        KinesisClientLibConfiguration kcfg = new KinesisClientLibConfiguration(args[0], args[1],
                DefaultAWSCredentialsProviderChain.getInstance(), UUID.randomUUID().toString());
        kcfg.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        MessageConsumer consumer = new KinesisConsumer(rpf, kcfg);
        consumer.startPolling();
        Thread.sleep(30000l);
        consumer.stopPolling();
    }

}
