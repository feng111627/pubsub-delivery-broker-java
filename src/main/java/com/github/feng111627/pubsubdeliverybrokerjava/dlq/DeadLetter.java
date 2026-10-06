package com.github.feng111627.pubsubdeliverybrokerjava.dlq;

/** 死信记录。 */
public record DeadLetter(String clientId, String messageId, String topic,
                         String reason, long deadAt) {
}
