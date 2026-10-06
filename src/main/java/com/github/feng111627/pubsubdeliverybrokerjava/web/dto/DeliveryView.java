package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

/**
 * 投递给消费端的一条消息。
 * @param deliveryAttempt 第几次投递（1 为首次，>1 表示重投）
 */
public record DeliveryView(String messageId, String topic, String payload,
                           long seq, int deliveryAttempt, boolean redelivery) {
}
