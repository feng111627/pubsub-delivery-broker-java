package com.github.feng111627.pubsubdeliverybrokerjava.service;

import com.github.feng111627.pubsubdeliverybrokerjava.config.BrokerProperties;
import com.github.feng111627.pubsubdeliverybrokerjava.dlq.DeadLetter;
import com.github.feng111627.pubsubdeliverybrokerjava.dlq.DeadLetterQueue;
import com.github.feng111627.pubsubdeliverybrokerjava.exception.AlreadyAckedException;
import com.github.feng111627.pubsubdeliverybrokerjava.exception.ApiException;
import com.github.feng111627.pubsubdeliverybrokerjava.exception.InvalidSubscriptionException;
import com.github.feng111627.pubsubdeliverybrokerjava.exception.MessageNotFoundException;
import com.github.feng111627.pubsubdeliverybrokerjava.model.BrokerMessage;
import com.github.feng111627.pubsubdeliverybrokerjava.model.PendingDelivery;
import com.github.feng111627.pubsubdeliverybrokerjava.model.Subscription;
import com.github.feng111627.pubsubdeliverybrokerjava.store.MessageStore;
import com.github.feng111627.pubsubdeliverybrokerjava.store.SubscriptionRegistry;
import com.github.feng111627.pubsubdeliverybrokerjava.topic.TopicMatcher;
import com.github.feng111627.pubsubdeliverybrokerjava.web.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Broker 核心编排。
 *
 * <p>并发模型：所有改变投递状态的操作都在一把进程内锁上串行执行。它带来三个关键保证：</p>
 * <ul>
 *   <li>同一主题高并发发布时，消息按到达顺序获得连续主题序号，投递顺序稳定；</li>
 *   <li>每个订阅端看到的全局序号只增不减，重连/重投均不会让进度回退；</li>
 *   <li>订阅端之间状态独立：慢客户端只在它自己的在途集合上累积/被驱逐，不影响他人。</li>
 * </ul>
 *
 * <p>投递语义：至少一次。首次投递可能超时重投（同一 messageId、deliveryAttempt 递增），
 * 因此消费端需要按 messageId 幂等；ACK 后消息立即离开在途路径，重复 ACK 明确报错。</p>
 */
@Service
public class BrokerService {

    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    static final String REASON_MAX_DELIVERIES = "MAX_DELIVERIES_EXCEEDED";
    static final String REASON_OVERFLOW = "PENDING_OVERFLOW";
    static final String STRATEGY_DEAD_LETTER = "dead-letter";
    static final String STRATEGY_DROP_OLDEST = "drop-oldest";

    private final Object lock = new Object();

    private final MessageStore messages;
    private final SubscriptionRegistry subscriptions;
    private final DeadLetterQueue dlq;
    private final BrokerProperties props;

    public BrokerService(MessageStore messages, SubscriptionRegistry subscriptions,
                         DeadLetterQueue dlq, BrokerProperties props) {
        this.messages = messages;
        this.subscriptions = subscriptions;
        this.dlq = dlq;
        this.props = props;
    }

    // ------------------------------------------------------------------ 发布

    public PublishResult publish(PublishRequest request) {
        if (request == null) {
            throw new InvalidSubscriptionException("请求体不能为空");
        }
        String topic = request.topic();
        String payload = request.payload() == null ? "" : request.payload();
        TopicMatcher.validatePublishTopic(topic);

        List<String> deadLetteredClients = new ArrayList<>();
        BrokerMessage msg;
        synchronized (lock) {
            msg = messages.append(topic, payload);
            for (Subscription sub : subscriptions.all()) {
                if (TopicMatcher.matches(sub.getFilter(), topic)) {
                    if (fanOutOne(sub, msg)) {
                        deadLetteredClients.add(sub.getClientId());
                    }
                }
            }
        }
        String status = "PUBLISHED";
        String note = null;
        if (!deadLetteredClients.isEmpty()) {
            status = "OVERFLOW_DEADLETTER";
            String joined = String.join(", ", deadLetteredClients);
            note = "订阅端 " + joined + " 在途积压达到上限 " + props.getMaxPendingPerSubscription()
                    + "（策略 " + props.getOverflowStrategy()
                    + "），该消息未进入其投递窗口；被拒绝/驱逐的消息均已进入死信，可在 /api/dead-letters 查询";
            log.info("消息 {} 对 {} 个积压订阅端按溢出策略处理: {}",
                    msg.id(), deadLetteredClients.size(), joined);
        }
        log.info("发布成功 topic={} id={} globalSeq={} topicSeq={}",
                topic, msg.id(), msg.globalSeq(), msg.topicSeq());
        return new PublishResult(msg.id(), msg.globalSeq(), msg.topicSeq(), status, note);
    }

    /**
     * 把一条匹配消息扇出给单个订阅端。调用方持锁。
     *
     * @return true 表示本条消息（新消息或被其顶掉的最旧消息）因积压溢出被写入死信
     */
    private boolean fanOutOne(Subscription sub, BrokerMessage msg) {
        long now = System.currentTimeMillis();
        if (sub.getPending().size() < props.getMaxPendingPerSubscription()) {
            sub.registerPending(msg, now);
            return false;
        }
        String strategy = props.getOverflowStrategy();
        if (STRATEGY_DROP_OLDEST.equals(strategy)) {
            // 驱逐最旧在途消息（进死信，可查），把位置让给新消息：优先追上最新流量
            Map.Entry<Long, PendingDelivery> oldest = sub.getPending().firstEntry();
            PendingDelivery evicted = oldest.getValue();
            sub.markDead(evicted.getMessage().globalSeq());
            dlq.record(new DeadLetter(sub.getClientId(), evicted.getMessage().id(),
                    evicted.getMessage().topic(),
                    REASON_OVERFLOW + ":drop-oldest(attempts=" + evicted.getAttempt() + ")",
                    now));
            sub.registerPending(msg, now);
            log.warn("订阅端 {} 积压超限，按 drop-oldest 将最旧消息 {} 移入死信，接收新消息 {}",
                    sub.getClientId(), evicted.getMessage().id(), msg.id());
            return true;
        }
        // 默认 dead-letter：保持现有投递窗口不动，拒绝新消息并入死信，优先保证窗口内消息
        sub.markDead(msg.globalSeq());
        dlq.record(new DeadLetter(sub.getClientId(), msg.id(), msg.topic(),
                REASON_OVERFLOW + ":dead-letter(pending=" + sub.getPending().size()
                        + ", limit=" + props.getMaxPendingPerSubscription() + ")", now));
        log.warn("订阅端 {} 积压超限，按 dead-letter 拒绝新消息 {} 并入死信",
                sub.getClientId(), msg.id());
        return true;
    }

    // ------------------------------------------------------------------ 订阅

    public SubscribeResponse subscribe(String clientId, String filter) {
        if (clientId == null || clientId.isBlank()) {
            throw new InvalidSubscriptionException("clientId 不能为空");
        }
        TopicMatcher.validateFilter(filter);
        long now = System.currentTimeMillis();
        synchronized (lock) {
            Subscription sub = subscriptions.getOrCreate(clientId, filter,
                    messages.currentGlobalSeq(), now);
            if (!sub.getFilter().equals(filter)) {
                throw new ApiException("SUBSCRIPTION_CONFLICT",
                        "clientId=" + clientId + " 已存在订阅 filter=" + sub.getFilter()
                                + "，与本次 filter=" + filter + " 不一致；同一 clientId 必须复用原 filter（重连请使用原 filter）");
            }
            log.info("订阅确认 clientId={} filter={} 当前已投递位置 deliveredSeq={}",
                    clientId, filter, sub.getDeliveredSeq());
            return new SubscribeResponse(clientId, filter, sub.getDeliveredSeq());
        }
    }

    // ------------------------------------------------------------------ 拉取

    public List<DeliveryView> poll(String clientId, int maxMessages) {
        Subscription sub = subscriptions.get(clientId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "订阅端不存在: clientId=" + clientId + "，请先调用 /api/subscribe"));
        int batch = Math.max(1, Math.min(maxMessages, 1000));
        long now = System.currentTimeMillis();
        List<DeliveryView> views = new ArrayList<>();
        synchronized (lock) {
            // 1) 优先投递"待重投"的在途消息（保持全局序号升序）
            for (PendingDelivery pd : new ArrayList<>(sub.getPending().values())) {
                if (views.size() >= batch) {
                    break;
                }
                if (pd.isDueForRedelivery()) {
                    pd.clearRedeliveryFlag();
                    views.add(toView(pd));
                }
            }
            // 2) 首次投递剩余的新在途消息，按序号升序
            for (PendingDelivery pd : new ArrayList<>(sub.getPending().values())) {
                if (views.size() >= batch) {
                    break;
                }
                if (pd.getAttempt() == 1 && !pd.isDueForRedelivery()) {
                    views.add(toView(pd));
                }
            }
            // 超时窗口从消息真正交到客户端手里起算：无论首次投递还是重投，发出即刷新计时
            for (DeliveryView v : views) {
                PendingDelivery pd = sub.getPending(v.seq());
                if (pd != null) {
                    pd.markSentAt(now);
                }
            }
        }
        if (!views.isEmpty()) {
            log.info("投递 clientId={} 数量={} 含重投={}",
                    clientId, views.size(), views.stream().anyMatch(DeliveryView::redelivery));
        }
        return views;
    }

    private DeliveryView toView(PendingDelivery pd) {
        BrokerMessage m = pd.getMessage();
        return new DeliveryView(m.id(), m.topic(), m.payload(), m.globalSeq(),
                pd.getAttempt(), pd.getAttempt() > 1);
    }

    // ------------------------------------------------------------------ ACK

    public void acknowledge(String clientId, String messageId) {
        if (messageId == null || messageId.isBlank()) {
            throw new InvalidSubscriptionException("ack.messageId 不能为空");
        }
        Subscription sub = subscriptions.get(clientId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "订阅端不存在: clientId=" + clientId));
        BrokerMessage msg = messages.find(messageId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "消息不存在: messageId=" + messageId));
        long seq = msg.globalSeq();
        synchronized (lock) {
            // 顺序敏感：在途 -> 已确认 -> 已死信 -> 从未投递给该订阅端
            if (sub.isPending(seq)) {
                sub.markAcked(seq);
                log.info("ACK 成功 clientId={} messageId={} seq={} 连续完成边界={}",
                        clientId, messageId, seq, sub.contiguousCompletedSeq());
                return;
            }
            if (sub.isAcked(seq)) {
                throw new AlreadyAckedException(
                        "消息已经确认过，不能重复确认: clientId=" + clientId
                                + ", messageId=" + messageId + ", seq=" + seq);
            }
            if (sub.isDead(seq)) {
                throw new ApiException("DEAD_LETTERED",
                        "消息已进入死信，无法再确认: clientId=" + clientId
                                + ", messageId=" + messageId
                                + "；请到 /api/dead-letters?clientId=" + clientId + " 查询");
            }
            throw new MessageNotFoundException(
                    "该消息从未投递给此订阅端（可能主题不匹配或订阅晚于发布）: clientId="
                            + clientId + ", messageId=" + messageId + ", topic=" + msg.topic());
        }
    }

    // ------------------------------------------------------- 投递续期

    /**
     * 投递续期：消费者在本次确认超时到点前申请把超时截止时刻往后延。
     *
     * <p>语义约束：</p>
     * <ul>
     *   <li>只延长计时，不把消息当已确认，不移除在途，不回退任何进度；</li>
     *   <li>续期只作用于"本次投递"：一旦超时进入重投，新投递窗口重新计算；</li>
     *   <li>续期后仍超时且未确认，按原规则重投，投递次数照常累计，到上限照常进死信。</li>
     * </ul>
     *
     * @param extendMs 续期时长；null 或 <=0 表示续一个完整 ACK 窗口
     */
    public HeartbeatResponse heartbeat(String clientId, String messageId, Long extendMs) {
        if (messageId == null || messageId.isBlank()) {
            throw new InvalidSubscriptionException("heartbeat.messageId 不能为空");
        }
        Subscription sub = subscriptions.get(clientId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "订阅端不存在: clientId=" + clientId + "，请先调用 /api/subscribe"));
        BrokerMessage msg = messages.find(messageId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "消息不存在: messageId=" + messageId));
        long extension = extendMs == null || extendMs <= 0 ? props.getAckTimeoutMs() : extendMs;
        if (extension > props.getHeartbeatMaxExtensionMs()) {
            throw new ApiException("INVALID_HEARTBEAT",
                    "续期时长 " + extension + "ms 超过上限 broker.heartbeat-max-extension-ms="
                            + props.getHeartbeatMaxExtensionMs()
                            + "ms；请分多次续期，防止消息被无限挂住");
        }
        long now = System.currentTimeMillis();
        synchronized (lock) {
            long seq = msg.globalSeq();
            PendingDelivery pd = sub.getPending(seq);
            if (pd != null) {
                if (!pd.isDelivered()) {
                    throw new ApiException("NOT_YET_DELIVERED",
                            "消息尚未被本订阅端拉取过，不存在需要续期的投递: clientId=" + clientId
                                    + ", messageId=" + messageId + "；请先 poll 取到消息后再续期");
                }
                long deadlineAt = now + extension;
                pd.extendDeadline(deadlineAt);
                log.info("续期成功 clientId={} messageId={} seq={} 延长 {}ms，新截止时刻={}",
                        clientId, messageId, seq, extension, deadlineAt);
                return new HeartbeatResponse(messageId, clientId, extension, deadlineAt);
            }
            if (sub.isAcked(seq)) {
                throw new AlreadyAckedException(
                        "消息已经确认过，无需续期: clientId=" + clientId
                                + ", messageId=" + messageId + ", seq=" + seq);
            }
            if (sub.isDead(seq)) {
                throw new ApiException("DEAD_LETTERED",
                        "消息已进入死信，本次投递已终结，无法续期: clientId=" + clientId
                                + ", messageId=" + messageId
                                + "；如需重新消费请使用 POST /api/dead-letters/redrive 再投递");
            }
            throw new MessageNotFoundException(
                    "该消息从未投递给此订阅端（可能主题不匹配或订阅晚于发布），无法续期: clientId="
                            + clientId + ", messageId=" + messageId + ", topic=" + msg.topic());
        }
    }

    // ------------------------------------------------------- 超时重投 / 死信

    /**
     * 扫描所有订阅端的在途消息：超过 ACK 超时窗口的按规则处理。
     * <ul>
     *   <li>投递次数未达上限：attempt+1，标记待重投（下一次 poll 优先返回）；</li>
     *   <li>达到上限：移出在途、写入死信台账（消息不丢失）。</li>
     * </ul>
     * 返回本轮处理条数（重投 + 死信）。
     */
    public int redeliverTimedOut() {
        long now = System.currentTimeMillis();
        long timeout = props.getAckTimeoutMs();
        int maxDeliveries = props.getMaxDeliveries();
        int processed = 0;
        synchronized (lock) {
            for (Subscription sub : subscriptions.all()) {
                for (PendingDelivery pd : new ArrayList<>(sub.getPending().values())) {
                    // 客户端尚未拉取过的消息不参与超时，避免"未投递先重投"；
                    // 续期过的消息按续期后的截止时刻判断，截止前不得误判超时
                    if (!pd.isDelivered() || now < pd.effectiveDeadline(timeout)) {
                        continue;
                    }
                    BrokerMessage msg = pd.getMessage();
                    if (pd.getAttempt() >= maxDeliveries) {
                        sub.markDead(msg.globalSeq());
                        dlq.record(new DeadLetter(sub.getClientId(), msg.id(), msg.topic(),
                                REASON_MAX_DELIVERIES + "(attempts=" + pd.getAttempt()
                                        + ", timeoutMs=" + timeout + ")",
                                now));
                        processed++;
                        log.warn("消息 {} 对订阅端 {} 投递 {} 次仍未 ACK，转入死信",
                                msg.id(), sub.getClientId(), pd.getAttempt());
                    } else {
                        pd.markRedelivery(now);
                        processed++;
                        log.info("消息 {} 对订阅端 {} ACK 超时，第 {} 次重投",
                                msg.id(), sub.getClientId(), pd.getAttempt());
                    }
                }
            }
        }
        return processed;
    }

    public List<DeadLetter> deadLetters(String clientId) {
        return clientId == null || clientId.isBlank()
                ? dlq.listAll()
                : dlq.listByClient(clientId);
    }

    /** 死信台账视图：附带"是否已被再投递"状态，便于运维判断哪些还没捞回。 */
    public List<DeadLetterView> deadLetterViews(String clientId) {
        List<DeadLetterView> out = new ArrayList<>();
        for (DeadLetter l : deadLetters(clientId)) {
            out.add(new DeadLetterView(l.clientId(), l.messageId(), l.topic(), l.reason(),
                    l.deadAt(), dlq.isRedriven(l.clientId(), l.messageId()),
                    dlq.redrivenAt(l.clientId(), l.messageId())));
        }
        return out;
    }

    // ------------------------------------------------------- 死信再投递

    /**
     * 死信再投递（redrive）：把一批死信消息重新投回其归属订阅端的在途窗口。
     *
     * <p>语义约束：</p>
     * <ul>
     *   <li>整批原子：任一条不合法（不存在/已确认/不在死信中）则整批拒绝，不产生部分生效；</li>
     *   <li>批内按全局序号（原发布顺序）重新入窗，重复触发同一批不会乱序也不会成倍重复
     *       ——第二次触发时消息已不在死信中，会被明确拒绝；</li>
     *   <li>容量确定：在途窗口放不下整批时明确拒绝（REDRIVE_CAPACITY_EXCEEDED），不悄悄丢消息；</li>
     *   <li>再投递的消息按全新一次投递计时（attempt=1），走正常 poll/ACK/超时重投流程。</li>
     * </ul>
     */
    public RedriveResponse redrive(String clientId, List<String> messageIds) {
        if (clientId == null || clientId.isBlank()) {
            throw new InvalidSubscriptionException("redrive.clientId 不能为空");
        }
        if (messageIds == null || messageIds.isEmpty()) {
            throw new InvalidSubscriptionException("redrive.messageIds 不能为空");
        }
        if (messageIds.size() > props.getMaxRedriveBatch()) {
            throw new ApiException("REDRIVE_BATCH_TOO_LARGE",
                    "单次再投递 " + messageIds.size() + " 条，超过上限 broker.max-redrive-batch="
                            + props.getMaxRedriveBatch() + "；请分批再投递");
        }
        Subscription sub = subscriptions.get(clientId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "订阅端不存在: clientId=" + clientId + "，请先调用 /api/subscribe"));
        long now = System.currentTimeMillis();
        synchronized (lock) {
            // 1) 整批校验：任何一条不合法都直接拒绝，状态不变
            List<BrokerMessage> batch = new ArrayList<>(messageIds.size());
            for (String id : messageIds) {
                BrokerMessage msg = messages.find(id)
                        .orElseThrow(() -> new MessageNotFoundException(
                                "消息不存在，无法再投递: messageId=" + id));
                long seq = msg.globalSeq();
                if (sub.isAcked(seq)) {
                    throw new AlreadyAckedException(
                            "消息已经确认过，无需再投递: clientId=" + clientId
                                    + ", messageId=" + id + ", seq=" + seq);
                }
                if (sub.isPending(seq)) {
                    throw new ApiException("NOT_DEAD_LETTERED",
                            "消息不在死信中（正在投递中或已被再投递），拒绝重复再投递: clientId=" + clientId
                                    + ", messageId=" + id + ", seq=" + seq);
                }
                if (!sub.isDead(seq)) {
                    throw new MessageNotFoundException(
                            "该消息从未投递给此订阅端，不在其死信中: clientId=" + clientId
                                    + ", messageId=" + id + ", topic=" + msg.topic());
                }
                batch.add(msg);
            }
            // 2) 容量检查：在途窗口必须放得下整批，否则明确拒绝（不截断、不丢消息）
            int free = props.getMaxPendingPerSubscription() - sub.getPending().size();
            if (batch.size() > free) {
                throw new ApiException("REDRIVE_CAPACITY_EXCEEDED",
                        "订阅端 " + clientId + " 在途窗口剩余容量 " + free + "，放不下本批 "
                                + batch.size() + " 条再投递（上限 broker.max-pending-per-subscription="
                                + props.getMaxPendingPerSubscription()
                                + "）；请先消费/确认部分在途消息后再试");
            }
            // 3) 按全局序号升序（原发布顺序）重新入窗；TreeMap 保证批内相对顺序稳定
            batch.sort(Comparator.comparingLong(BrokerMessage::globalSeq));
            List<String> ids = new ArrayList<>(batch.size());
            List<Long> seqs = new ArrayList<>(batch.size());
            for (BrokerMessage msg : batch) {
                sub.markRedriven(msg, now);
                dlq.markRedriven(clientId, msg.id(), now);
                ids.add(msg.id());
                seqs.add(msg.globalSeq());
            }
            log.info("死信再投递 clientId={} 数量={} seqs={}", clientId, ids.size(), seqs);
            return new RedriveResponse(clientId, ids.size(), ids, seqs);
        }
    }
}
