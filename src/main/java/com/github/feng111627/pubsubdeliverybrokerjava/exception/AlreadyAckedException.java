package com.github.feng111627.pubsubdeliverybrokerjava.exception;

/** 对已经确认过的投递重复确认。 */
public class AlreadyAckedException extends ApiException {
    public AlreadyAckedException(String message) {
        super("ALREADY_ACKED", message);
    }
}
