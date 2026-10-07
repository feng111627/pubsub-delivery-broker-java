package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

import java.util.List;

/** 死信再投递请求：把指定死信消息重新投给对应订阅端。 */
public record RedeliverDeadLettersRequest(String clientId, List<String> messageIds) { }
