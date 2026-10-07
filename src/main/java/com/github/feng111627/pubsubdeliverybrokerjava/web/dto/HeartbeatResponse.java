package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

/**
 * 投递续期结果。
 *
 * @param messageId  被续期的消息
 * @param clientId   订阅端
 * @param extendMs   实际续期时长（毫秒）
 * @param deadlineAt 新的绝对超时截止时刻（epoch 毫秒）；此前不会被判定超时重投
 */
public record HeartbeatResponse(String messageId, String clientId, long extendMs, long deadlineAt) {
}
