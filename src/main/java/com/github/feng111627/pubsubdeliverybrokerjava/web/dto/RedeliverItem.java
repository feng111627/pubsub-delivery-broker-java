package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

/**
 * 批量再投递中单条消息的处理结果。
 * @param status REDELIVERED（本次捞回）/ ALREADY_REDELIVERED（之前已捞回，幂等不再重复投）
 */
public record RedeliverItem(String messageId, long seq, String status) { }
