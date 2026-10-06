package com.github.feng111627.pubsub.api;

import com.github.feng111627.pubsub.broker.BrokerException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BrokerException.class)
    public ResponseEntity<Map<String, Object>> handle(BrokerException ex) {
        HttpStatus status = switch (ex.code()) {
            case INVALID_TOPIC -> HttpStatus.BAD_REQUEST;
            case SUBSCRIPTION_NOT_FOUND, DELIVERY_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case ALREADY_ACKED -> HttpStatus.CONFLICT;
            case BACKLOG_FULL -> HttpStatus.TOO_MANY_REQUESTS;
        };
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", ex.code().name());
        body.put("message", ex.getMessage());
        return ResponseEntity.status(status).body(body);
    }
}
