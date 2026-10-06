package com.github.feng111627.pubsub.broker;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 一个具名订阅端。按名字持久（进程内），断线重连后从 cursor 继续。
 * 各订阅端持有独立队列，互不拖累。
 */
public class Subscription {

    private final String name;
    private final Topic filter;
    private final Deque<Delivery> pending = new ArrayDeque<>();
    private final Map<String, Delivery> inFlight = new HashMap<>();
    /** 每个主题已确认的最大序号，进度只允许前进。 */
    private final Map<String, Long> ackedSeqByTopic = new HashMap<>();
    /** 已确认的投递 id，用于对重复确认明确报错。 */
    private final Set<String> ackedDeliveryIds = new HashSet<>();

    public Subscription(String name, Topic filter) {
        this.name = name;
        this.filter = filter;
    }

    public String name() { return name; }
    public Topic filter() { return filter; }
    Deque<Delivery> pending() { return pending; }
    Map<String, Delivery> inFlight() { return inFlight; }
    Set<String> ackedDeliveryIds() { return ackedDeliveryIds; }

    public long ackedSeqOf(String topic) {
        return ackedSeqByTopic.getOrDefault(topic, 0L);
    }

    /**
     * 推进某主题的已确认进度。只允许前进：
     * 对旧消息的迟到确认不会把进度拉回去。
     */
    void advanceCursor(String topic, long seq) {
        ackedSeqByTopic.merge(topic, seq, Math::max);
    }
}
