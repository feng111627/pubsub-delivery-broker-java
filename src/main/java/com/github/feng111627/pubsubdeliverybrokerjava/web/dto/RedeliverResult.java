package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

import java.util.List;

/** 死信再投递结果：items 顺序与请求中的 messageIds 顺序一致。 */
public record RedeliverResult(String clientId, int redelivered, int alreadyRedelivered,
                              List<RedeliverItem> items) { }
