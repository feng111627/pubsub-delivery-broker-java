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
    /** 续期到的绝对时间戳（毫秒），0 表示没有续期。只作用于本次投递窗口。 */
    private long extendedUntil;

    public PendingDelivery(BrokerMessage message, int attempt, long sentAt) {
        this.message = message;
        this.attempt = attempt;
        this.lastSentAt = sentAt;
        this.dueForRedelivery = false;
        this.delivered = false;
        this.extendedUntil = 0;
    }

    public boolean isDelivered() { return delivered; }

    public BrokerMessage getMessage() { return message; }

    public int getAttempt() { return attempt; }

    public long getLastSentAt() { return lastSentAt; }

    public boolean isDueForRedelivery() { return dueForRedelivery; }

    /** 标记为需要重投，并递增投递次数、刷新发送时间（超时窗口重新计算，续期随之失效）。 */
    public void markRedelivery(long now) {
        this.attempt++;
        this.lastSentAt = now;
        this.dueForRedelivery = true;
        this.extendedUntil = 0;
    }

    /** 首次被 poll 取出时刷新计时：超时窗口从客户端真正拿到消息起算。 */
    public void markSentAt(long now) {
        this.lastSentAt = now;
        this.delivered = true;
        this.extendedUntil = 0;
    }

    /** 已在一次 poll 中把重投副本交给客户端，清除待重投标记。 */
    public void clearRedeliveryFlag() {
        this.dueForRedelivery = false;
    }

    /** 死信再投递时直接排入"待重投"队列，让下一次 poll 优先投出。 */
    public void markQueuedForRedelivery() {
        this.dueForRedelivery = true;
    }

    /**
     * 投递续期：把本次投递的超时到点往后延。只延长、不缩短（单调），
     * 不触碰 attempt / delivered 等任何其他状态，因此不等于确认、也不会让消息消失。
     */
    public void extend(long now, long extendMs) {
        this.extendedUntil = Math.max(this.extendedUntil, now + extendMs);
    }

    public long getExtendedUntil() { return extendedUntil; }

    /** 本次投递的实际超时到点：正常窗口与续期到点取较大者。 */
    public long effectiveDeadline(long timeoutMs) {
        return Math.max(lastSentAt + timeoutMs, extendedUntil);
    }
}
