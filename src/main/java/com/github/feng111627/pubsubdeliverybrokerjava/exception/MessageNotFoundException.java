package com.github.feng111627.pubsubdeliverybrokerjava.exception;

/** ACK 指向不存在的投递/消息。 */
public class MessageNotFoundException extends ApiException {
    public MessageNotFoundException(String message) {
        super("MESSAGE_NOT_FOUND", message);
    }
}
