package com.github.feng111627.pubsubdeliverybrokerjava.model;

/**
 * 已发布的消息（不可变）。
 *
 * @param globalSeq   Broker 全局单调序号
 * @param topicSeq    主题内单调序号（用于同主题稳定投递顺序）
 */
public record BrokerMessage(String id, String topic, String payload, long publishTime,
                            long globalSeq, long topicSeq) {
}
