package com.github.feng111627.pubsub.broker;

/** 业务错误，携带稳定的错误码，便于 API 层映射为明确响应。 */
public class BrokerException extends RuntimeException {

    public enum Code {
        INVALID_TOPIC,
        SUBSCRIPTION_NOT_FOUND,
        DELIVERY_NOT_FOUND,
        ALREADY_ACKED,
        BACKLOG_FULL
    }

    private final Code code;

    public BrokerException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code code() { return code; }
}
