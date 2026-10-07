package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

/**
 * 投递续期请求。
 *
 * @param messageId 要续期的消息（必须已投递给该订阅端且仍在途）
 * @param extendMs  续期时长（毫秒）；为空或 <=0 时按 broker.ack-timeout-ms 续一个完整窗口，
 *                  上限为 broker.heartbeat-max-extension-ms
 */
public record HeartbeatRequest(String messageId, Long extendMs) {
}
