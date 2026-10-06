package com.github.feng111627.pubsubdeliverybrokerjava.exception;

/** 业务异常基类：带稳定错误码，便于客户端识别。 */
public class ApiException extends RuntimeException {
    private final String errorCode;

    public ApiException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public String getErrorCode() { return errorCode; }
}
