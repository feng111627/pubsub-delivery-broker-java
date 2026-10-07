package com.github.feng111627.pubsubdeliverybrokerjava.model;

/**
 * 一条"在途投递"：消息已经发给某个订阅端、但尚未收到 ACK。
 *
 * <p>可变状态仅在 BrokerService 的全局锁内修改。</p>
 */
public class PendingDelivery {

    private final BrokerMessage message;
    private int attempt;
    private long lastSentAt;
    private boolean dueForRedelivery;
    /** 是否已真正发给客户端（首次 poll 取出）；此前不参与超时重投。 */
    private boolean delivered;
    /**
     * 续期后的绝对超时截止时刻（epoch 毫秒）；<=0 表示未续期，
     * 此时超时窗口按 {@code lastSentAt + ackTimeoutMs} 计算。
     * 每次重新发送（首次取出/重投）都会清零，续期只对"本次投递"有效。
     */
    private long deadlineOverride;

    public PendingDelivery(BrokerMessage message, int attempt, long sentAt) {
        this.message = message;
        this.attempt = attempt;
        this.lastSentAt = sentAt;
        this.dueForRedelivery = false;
        this.delivered = false;
        this.deadlineOverride = 0;
    }

    public boolean isDelivered() { return delivered; }

    public BrokerMessage getMessage() { return message; }

    public int getAttempt() { return attempt; }

    public long getLastSentAt() { return lastSentAt; }

    public boolean isDueForRedelivery() { return dueForRedelivery; }

    public long getDeadlineOverride() { return deadlineOverride; }

    /**
     * 计算本次投递的绝对超时截止时刻。
     * 续期过则按续期截止时刻，否则按 {@code lastSentAt + ackTimeoutMs}。
     */
    public long effectiveDeadline(long ackTimeoutMs) {
        return deadlineOverride > 0 ? deadlineOverride : lastSentAt + ackTimeoutMs;
    }

    /**
     * 投递续期：把本次投递的超时截止时刻推迟到 {@code newDeadlineAt}。
     * 仅延长计时，不改变确认/在途状态，也不递增投递次数。
     */
    public void extendDeadline(long newDeadlineAt) {
        this.deadlineOverride = newDeadlineAt;
    }

    /** 标记为需要重投，并递增投递次数、刷新发送时间（超时窗口重新计算，续期失效）。 */
    public void markRedelivery(long now) {
        this.attempt++;
        this.lastSentAt = now;
        this.dueForRedelivery = true;
        this.deadlineOverride = 0;
    }

    /** 首次被 poll 取出时刷新计时：超时窗口从客户端真正拿到消息起算（续期失效）。 */
    public void markSentAt(long now) {
        this.lastSentAt = now;
        this.delivered = true;
        this.deadlineOverride = 0;
    }

    /** 已在一次 poll 中把重投副本交给客户端，清除待重投标记。 */
    public void clearRedeliveryFlag() {
        this.dueForRedelivery = false;
    }
}
