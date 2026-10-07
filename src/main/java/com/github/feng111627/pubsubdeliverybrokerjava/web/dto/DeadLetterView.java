package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

/**
 * 死信台账视图（含再投递状态）。
 *
 * @param redriven   是否已被再投递回订阅端
 * @param redrivenAt 再投递时刻（epoch 毫秒），未再投递时为 null
 */
public record DeadLetterView(String clientId, String messageId, String topic,
                             String reason, long deadAt,
                             boolean redriven, Long redrivenAt) {
}
