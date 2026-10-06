package com.github.feng111627.pubsub.broker;

import java.time.Instant;

/** 一条已发布的消息。seq 为主题内单调递增序号，保证同主题投递顺序稳定。 */
public record Message(String id, Topic topic, long seq, String payload, Instant publishedAt) {
}
