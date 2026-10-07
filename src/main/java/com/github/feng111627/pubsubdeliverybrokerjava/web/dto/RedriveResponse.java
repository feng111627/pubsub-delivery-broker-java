package com.github.feng111627.pubsubdeliverybrokerjava.web.dto;

import java.util.List;

/**
 * 死信再投递结果。
 *
 * @param clientId   目标订阅端
 * @param count      成功再投递的条数
 * @param messageIds 再投递的消息 ID（按全局序号升序，即原发布顺序）
 * @param seqs       对应的全局序号（升序）
 */
public record RedriveResponse(String clientId, int count,
                              List<String> messageIds, List<Long> seqs) {
}
