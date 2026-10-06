package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

/**
 * @param status PUBLISHED / OVERFLOW_DEADLETTER
 */
public record PublishResult(String messageId, long globalSeq, long topicSeq,
                            String status, String note) {
}
