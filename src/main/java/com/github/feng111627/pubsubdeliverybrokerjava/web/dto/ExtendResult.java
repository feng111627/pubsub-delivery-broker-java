package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

/**
 * 投递续期结果。
 * @param extendedUntil 本次投递新的超时到点（绝对时间戳，毫秒）
 */
public record ExtendResult(String messageId, long seq, long extendedUntil) { }
