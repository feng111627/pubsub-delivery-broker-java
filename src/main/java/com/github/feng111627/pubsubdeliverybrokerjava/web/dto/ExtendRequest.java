package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

/**
 * 投递续期请求。
 * @param extendMs 往后延长的毫秒数；为空时按 broker.ack-timeout-ms 延长
 */
public record ExtendRequest(String messageId, Long extendMs) { }
