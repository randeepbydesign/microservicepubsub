package com.randeepbydesign.pubsub;

/**
 * Generic envelope representing a published/consumed message
 */
public class Message {
    private String messageId;
    private String subject;
    private String messageType;
    private String message;

    public Message(String messageId, String subject, String messageType, String message) {
        this.subject = subject;
        this.messageType = messageType;
        this.message = message;
        this.messageId = messageId;
    }

    public Message(String messageId, String messageType, String message) {
        this.messageType = messageType;
        this.message = message;
        this.messageId = messageId;
    }

    public Message(String messageId, String message) {
        this.message = message;
        this.messageId = messageId;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getSubject() {
        return subject;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getMessage() {
        return message;
    }

}
