package com.randeepbydesign.pubsub;

import com.amazonaws.services.sqs.model.Message;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple message processor that extracts the subject and message and writes that information to the console.
 */
public class PrintlnProcessor implements MessageProcessor {

    private static final Logger log = Logger.getAnonymousLogger();

    private static final Pattern DATA_EXTRACTOR = Pattern
            .compile(".*\\\"Subject\\\"\\s*:\\s*\\\"([^\"]+)\\\".*\\\"Message\\\"\\s*:\\s*\\\"([^\"]+)\\\".+$",
                    Pattern.DOTALL);

    @Override
    public String processMessage(Message message) {
        Matcher m = DATA_EXTRACTOR.matcher(message.getBody());
        if (!m.matches() || m.groupCount() != 2) {
            throw new RuntimeException("Unable to parse message");
        }
        System.out.println("Parsing message:\n\t" + m.group(2));
        return message.getReceiptHandle();
    }

}
