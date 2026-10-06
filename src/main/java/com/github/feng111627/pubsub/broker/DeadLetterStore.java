package com.github.feng111627.pubsub.broker;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 死信存储：重投超限或积压溢出的消息落到这里，可查询，不会凭空消失。 */
public class DeadLetterStore {

    public record DeadLetter(String deliveryId, String subscription, Message message,
                             String reason, int attempts, Instant deadAt) {
    }

    private final List<DeadLetter> store = new CopyOnWriteArrayList<>();

    public void add(String subscription, Delivery delivery, String reason) {
        store.add(new DeadLetter(delivery.id(), subscription, delivery.message(),
                reason, delivery.attempts(), Instant.now()));
    }

    public List<DeadLetter> list() {
        return List.copyOf(store);
    }

    public List<DeadLetter> listBySubscription(String subscription) {
        return store.stream().filter(d -> d.subscription().equals(subscription)).toList();
    }
}
