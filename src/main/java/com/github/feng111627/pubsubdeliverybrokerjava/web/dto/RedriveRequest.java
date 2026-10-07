package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

import java.util.List;

/**
 * 死信再投递请求（批量，原子生效：任一条不合法则整批拒绝）。
 *
 * @param clientId   目标订阅端（死信归属的订阅端）
 * @param messageIds 要再投递的消息 ID 列表；批内相对顺序按原发布顺序（全局序号）稳定
 */
public record RedriveRequest(String clientId, List<String> messageIds) {
}
