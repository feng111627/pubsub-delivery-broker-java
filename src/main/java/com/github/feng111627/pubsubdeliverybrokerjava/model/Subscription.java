package com.github.feng111627.pubsubdeliverybrokerjava.model;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/**
 * 一个订阅端及其投递状态。
 *
 * <p>核心语义（对外部可见的消费进度永远单调不回退）：</p>
 * <ul>
 *   <li>{@code deliveredSeq}：已交给本订阅端的最大全局序号（含已确认与在途），只增不减。</li>
 *   <li>{@code pending}：在途（未确认且未死信）投递，按全局序号索引。</li>
 *   <li>{@code ackedSeqs}：已确认序号集合，重复确认据此报错（ALREADY_ACKED）。</li>
 *   <li>{@code deadSeqs}：已转入死信的序号集合，消息不再投递，但进度仍已越过它。</li>
 * </ul>
 *
 * <p>所有方法仅在 BrokerService 全局锁内调用。</p>
 */
public class Subscription {

    private final String clientId;
    private final String filter;
    private final long createdAt;

    private long deliveredSeq;
    private final TreeMap<Long, PendingDelivery> pending = new TreeMap<>();
    private final Set<Long> ackedSeqs = new HashSet<>();
    private final Set<Long> deadSeqs = new HashSet<>();

    public Subscription(String clientId, String filter, long startSeq, long createdAt) {
        this.clientId = clientId;
        this.filter = filter;
        this.deliveredSeq = startSeq;
        this.createdAt = createdAt;
    }

    public String getClientId() { return clientId; }

    public String getFilter() { return filter; }

    public long getCreatedAt() { return createdAt; }

    public long getDeliveredSeq() { return deliveredSeq; }

    public TreeMap<Long, PendingDelivery> getPending() { return pending; }

    public Set<Long> getAckedSeqs() { return ackedSeqs; }

    public Set<Long> getDeadSeqs() { return deadSeqs; }

    /** 把一条新消息登记为在途，并推进已投递游标。 */
    public void registerPending(BrokerMessage msg, long now) {
        pending.put(msg.globalSeq(), new PendingDelivery(msg, 1, now));
        if (msg.globalSeq() > deliveredSeq) {
            deliveredSeq = msg.globalSeq();
        }
    }

    public PendingDelivery getPending(long seq) {
        return pending.get(seq);
    }

    public PendingDelivery removePending(long seq) {
        return pending.remove(seq);
    }

    public void markAcked(long seq) {
        pending.remove(seq);
        deadSeqs.remove(seq);
        ackedSeqs.add(seq);
    }

    public void markDead(long seq) {
        pending.remove(seq);
        ackedSeqs.remove(seq);
        deadSeqs.add(seq);
    }

    /**
     * 死信再投递：把一条已死信的消息重新放回在途集合，按新一轮投递周期处理
     * （attempt 从 1 重新计），并排入待重投队列让下一次 poll 优先投出。
     * 消息保持原 globalSeq/messageId，deliveredSeq 游标只增不减、不会回退。
     */
    public void revive(BrokerMessage msg, long now) {
        long seq = msg.globalSeq();
        deadSeqs.remove(seq);
        PendingDelivery pd = new PendingDelivery(msg, 1, now);
        pd.markQueuedForRedelivery();
        pending.put(seq, pd);
    }

    public boolean isAcked(long seq) { return ackedSeqs.contains(seq); }
    public boolean isDead(long seq) { return deadSeqs.contains(seq); }
    public boolean isPending(long seq) { return pending.containsKey(seq); }

    /**
     * 连续完成（已确认或已死信）边界：从最低已投递序号开始连续完成的最大序号。
     * 这是消费端可对外宣称的"已完成进度"，只会前进不会回退。
     */
    public long contiguousCompletedSeq() {
        // 投递严格按序号升序：若在途集合为空，则 1..deliveredSeq 全部完成；
        // 否则最小在途序号 p 之前的所有消息必然已完成，边界为 p-1。
        return pending.isEmpty() ? deliveredSeq : pending.firstKey() - 1;
    }
}
