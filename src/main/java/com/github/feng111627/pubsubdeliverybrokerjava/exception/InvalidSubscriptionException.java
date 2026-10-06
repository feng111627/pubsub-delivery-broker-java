package com.github.feng111627.pubsubdeliverybrokerjava.exception;

/** 订阅请求非法（filter 语法错误 / 发布主题非法等）。 */
public class InvalidSubscriptionException extends ApiException {
    public InvalidSubscriptionException(String message) {
        super("INVALID_SUBSCRIPTION", message);
    }
}
