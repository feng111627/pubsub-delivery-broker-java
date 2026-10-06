package com.github.feng111627.pubsub.broker;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 核心 Broker。
 * 顺序：同一主题的序号在条纹锁内分配并入队，高并发发布下同主题投递顺序稳定。
 * 隔离：每个订阅端独立队列与锁，慢订阅端只影响自己的积压。
 */
@Service
public class Broker {

    private final BrokerProperties props;
    private final DeadLetterStore deadLetterStore;
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> topicSeq = new ConcurrentHashMap<>();
    private final Object[] topicLocks = new Object[64];

    public Broker(BrokerProperties props, DeadLetterStore deadLetterStore) {
        this.props = props;
        this.deadLetterStore = deadLetterStore;
        for (int i = 0; i < topicLocks.length; i++) {
            topicLocks[i] = new Object();
        }
    }

    /** 发布一条消息，扇出到所有匹配订阅。 */
    public Message publish(String topicName, String payload) {
        Topic topic = Topic.parsePublishTopic(topicName);
        Object lock = topicLocks[(topicName.hashCode() & 0x7fffffff) % topicLocks.length];
        synchronized (lock) {
            long seq = topicSeq.computeIfAbsent(topicName, k -> new AtomicLong()).incrementAndGet();
            Message message = new Message(UUID.randomUUID().toString(), topic, seq, payload, Instant.now());
            List<Subscription> targets = subscriptions.values().stream()
                    .filter(s -> s.filter().matches(topic))
                    .toList();
            if (props.getBacklogStrategy() == BrokerProperties.BacklogStrategy.REJECT) {
                for (Subscription s : targets) {
                    synchronized (s) {
                        if (backlogOf(s) >= props.getMaxBacklog()) {
                            throw new BrokerException(BrokerException.Code.BACKLOG_FULL,
                                    "订阅端 '" + s.name() + "' 积压已达上限 " + props.getMaxBacklog()
                                            + "，发布被拒绝（策略=REJECT）");
                        }
                    }
                }
            }
            for (Subscription s : targets) {
                enqueue(s, new Delivery(UUID.randomUUID().toString(), message));
            }
            return message;
        }
    }

    private void enqueue(Subscription sub, Delivery delivery) {
        synchronized (sub) {
            if (backlogOf(sub) >= props.getMaxBacklog()) {
                // 策略=DEAD_LETTER：溢出消息不丢也不阻塞，落死信可查。
                delivery.markDead();
                deadLetterStore.add(sub.name(), delivery, "BACKLOG_OVERFLOW");
                return;
            }
            sub.pending().addLast(delivery);
        }
    }

    private static int backlogOf(Subscription s) {
        return s.pending().size() + s.inFlight().size();
    }

    /**
     * 创建或重连一个具名订阅。
     * 重连（同名同过滤器）时保留进度，并把断线期间未确认的投递退回队列立即重投。
     */
    public Subscription subscribe(String name, String filterName) {
        if (name == null || name.isBlank()) {
            throw new BrokerException(BrokerException.Code.INVALID_TOPIC, "订阅端名字不能为空");
        }
        Topic filter = Topic.parseFilter(filterName);
        Subscription existing = subscriptions.get(name);
        if (existing != null) {
            if (!existing.filter().equals(filter)) {
                throw new BrokerException(BrokerException.Code.INVALID_TOPIC,
                        "订阅端 '" + name + "' 已存在，过滤器 '" + existing.filter()
                                + "' 与请求 '" + filterName + "' 不一致；如需变更请先换名订阅");
            }
            synchronized (existing) {
                requeueInFlightLocked(existing);
            }
            return existing;
        }
        Subscription created = new Subscription(name, filter);
        Subscription raced = subscriptions.putIfAbsent(name, created);
        return raced != null ? raced : created;
    }

    /** 拉取最多 max 条投递，置为 IN_FLIGHT 并启动确认计时。 */
    public List<Delivery> poll(String subscriptionName, int max) {
        Subscription sub = requireSubscription(subscriptionName);
        List<Delivery> out = new ArrayList<>();
        synchronized (sub) {
            while (out.size() < max && !sub.pending().isEmpty()) {
                Delivery d = sub.pending().pollFirst();
                d.markInFlight(Instant.now().plus(props.getAckTimeout()));
                sub.inFlight().put(d.id(), d);
                out.add(d);
            }
        }
        return out;
    }

    /** 确认一条投递；不存在或已确认会明确报错。 */
    public void ack(String subscriptionName, String deliveryId) {
        Subscription sub = requireSubscription(subscriptionName);
        synchronized (sub) {
            Delivery d = sub.inFlight().remove(deliveryId);
            if (d == null) {
                if (sub.ackedDeliveryIds().contains(deliveryId)) {
                    throw new BrokerException(BrokerException.Code.ALREADY_ACKED,
                            "投递 '" + deliveryId + "' 已确认过，不能重复确认");
                }
                throw new BrokerException(BrokerException.Code.DELIVERY_NOT_FOUND,
                        "订阅端 '" + subscriptionName + "' 不存在待确认的投递 '" + deliveryId + "'");
            }
            d.markAcked();
            sub.ackedDeliveryIds().add(deliveryId);
            sub.advanceCursor(d.message().topic().toString(), d.message().seq());
        }
    }

    /** 扫描超时未确认的投递：未超上限则重投，超上限落死信。返回本次处理的投递数。 */
    public int redeliverExpired() {
        int handled = 0;
        Instant now = Instant.now();
        for (Subscription sub : subscriptions.values()) {
            synchronized (sub) {
                List<Delivery> expired = sub.inFlight().values().stream()
                        .filter(d -> d.ackDeadline() != null && d.ackDeadline().isBefore(now))
                        .sorted(Comparator.comparing((Delivery d) -> d.message().topic().toString())
                                .thenComparingLong(d -> d.message().seq())
                                .reversed()) // 倒序 addFirst，队首仍是最小序号
                        .toList();
                for (Delivery d : expired) {
                    sub.inFlight().remove(d.id());
                    if (d.attempts() >= props.getMaxDeliveryAttempts()) {
                        d.markDead();
                        deadLetterStore.add(sub.name(), d, "MAX_DELIVERY_ATTEMPTS_EXCEEDED");
                    } else {
                        d.backToPending();
                        sub.pending().addFirst(d); // 重投的排在前面，尽量保持顺序
                    }
                    handled++;
                }
            }
        }
        return handled;
    }

    /** 断线重连时把未确认的投递按序退回待投递队列。 */
    private void requeueInFlightLocked(Subscription sub) {
        List<Delivery> inflight = sub.inFlight().values().stream()
                .sorted(Comparator.comparing((Delivery d) -> d.message().topic().toString())
                        .thenComparingLong(d -> d.message().seq())
                        .reversed()) // 倒序 addFirst，队首仍是最小序号
                .toList();
        for (Delivery d : inflight) {
            sub.inFlight().remove(d.id());
            d.backToPending();
            sub.pending().addFirst(d);
        }
    }

    private Subscription requireSubscription(String name) {
        Subscription sub = subscriptions.get(name);
        if (sub == null) {
            throw new BrokerException(BrokerException.Code.SUBSCRIPTION_NOT_FOUND,
                    "订阅端 '" + name + "' 不存在，请先订阅");
        }
        return sub;
    }

    public Subscription getSubscription(String name) {
        return subscriptions.get(name);
    }
}
