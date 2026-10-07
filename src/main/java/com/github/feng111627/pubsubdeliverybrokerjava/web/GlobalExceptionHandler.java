package com.github.feng111627.pubsubdeliverybrokerjava.web;

import com.github.feng111627.pubsubdeliverybrokerjava.exception.ApiException;
import com.github.feng111627.pubsubdeliverybrokerjava.web.dto.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 统一错误响应：错误码 + 明确原因，绝不把业务错误静默当成功。 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> handleApi(ApiException ex) {
        HttpStatus status = switch (ex.getErrorCode()) {
            case "SUBSCRIPTION_CONFLICT" -> HttpStatus.CONFLICT;
            case "PENDING_FULL" -> HttpStatus.TOO_MANY_REQUESTS;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status)
                .body(new ErrorResponse(ex.getErrorCode(), ex.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse("MALFORMED_REQUEST", "请求体不是合法 JSON 或缺少字段: "
                        + ex.getMostSpecificCause().getMessage()));
    }
}
