package com.randeepbydesign.pubsub;

public interface Publisher {

    /**
     *
     * @param subject
     * @param messageBody
     * @return an implementation-specific identifier for the published messaage
     */
    String publish(final String subject, final String messageBody);

    String publishObject(final String subject, final Object message);
}
