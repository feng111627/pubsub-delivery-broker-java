package com.github.feng111627.pubsub.broker;

import java.time.Instant;

/**
 * 某订阅下一条消息的投递状态机。
 * PENDING -> IN_FLIGHT -> ACKED；IN_FLIGHT 超时 -> PENDING（重投）；超过上限 -> DEAD。
 */
public class Delivery {

    private final String id;
    private final Message message;
    private DeliveryStatus status;
    private int attempts;
    private Instant ackDeadline;

    public Delivery(String id, Message message) {
        this.id = id;
        this.message = message;
        this.status = DeliveryStatus.PENDING;
        this.attempts = 0;
    }

    public String id() { return id; }
    public Message message() { return message; }
    public DeliveryStatus status() { return status; }
    public int attempts() { return attempts; }
    public Instant ackDeadline() { return ackDeadline; }

    void markInFlight(Instant deadline) {
        if (status != DeliveryStatus.PENDING) {
            throw new IllegalStateException("只有 PENDING 才能投递，当前=" + status);
        }
        status = DeliveryStatus.IN_FLIGHT;
        attempts++;
        ackDeadline = deadline;
    }

    void markAcked() {
        if (status != DeliveryStatus.IN_FLIGHT) {
            throw new IllegalStateException("只有 IN_FLIGHT 才能确认，当前=" + status);
        }
        status = DeliveryStatus.ACKED;
        ackDeadline = null;
    }

    void markDead() {
        status = DeliveryStatus.DEAD;
        ackDeadline = null;
    }

    /** 超时未确认，退回待投递队列等待重投。 */
    void backToPending() {
        if (status != DeliveryStatus.IN_FLIGHT) {
            throw new IllegalStateException("只有 IN_FLIGHT 才能退回重投，当前=" + status);
        }
        status = DeliveryStatus.PENDING;
        ackDeadline = null;
    }
}
