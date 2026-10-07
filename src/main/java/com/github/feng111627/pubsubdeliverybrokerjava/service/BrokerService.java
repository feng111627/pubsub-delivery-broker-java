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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
                    now, 0));
            sub.registerPending(msg, now);
            log.warn("订阅端 {} 积压超限，按 drop-oldest 将最旧消息 {} 移入死信，接收新消息 {}",
                    sub.getClientId(), evicted.getMessage().id(), msg.id());
            return true;
        }
        // 默认 dead-letter：保持现有投递窗口不动，拒绝新消息并入死信，优先保证窗口内消息
        sub.markDead(msg.globalSeq());
        dlq.record(new DeadLetter(sub.getClientId(), msg.id(), msg.topic(),
                REASON_OVERFLOW + ":dead-letter(pending=" + sub.getPending().size()
                        + ", limit=" + props.getMaxPendingPerSubscription() + ")", now, 0));
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
            // 1) 优先投递"待重投"的在途消息（保持全局序号升序）。
            //    注意此处只收集不清标记：attempt=1 的捞回消息也带待重投标记，
            //    若先清标记，第二轮会把它再次当成首次投递重复发出。
            List<PendingDelivery> due = new ArrayList<>();
            for (PendingDelivery pd : sub.getPending().values()) {
                if (due.size() >= batch) {
                    break;
                }
                if (pd.isDueForRedelivery()) {
                    due.add(pd);
                }
            }
            for (PendingDelivery pd : due) {
                views.add(toView(pd));
            }
            // 2) 首次投递剩余的新在途消息，按序号升序（跳过仍带待重投标记的）
            for (PendingDelivery pd : sub.getPending().values()) {
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
                    pd.clearRedeliveryFlag();
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
                    // 续期会把到点往后推，续期窗口未到期前不得误判超时
                    if (!pd.isDelivered() || now < pd.effectiveDeadline(timeout)) {
                        continue;
                    }
                    BrokerMessage msg = pd.getMessage();
                    if (pd.getAttempt() >= maxDeliveries) {
                        sub.markDead(msg.globalSeq());
                        dlq.record(new DeadLetter(sub.getClientId(), msg.id(), msg.topic(),
                                REASON_MAX_DELIVERIES + "(attempts=" + pd.getAttempt()
                                        + ", timeoutMs=" + timeout + ")",
                                now, 0));
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

    // ------------------------------------------------------- 投递续期

    /**
     * 投递续期：消费者在本次确认超时到点前申请把超时往后延。
     *
     * <p>只延长在途消息的超时到点，不等于确认、不改变投递次数、不影响消费进度。
     * 续期窗口到期后仍未 ACK 的，按原规则进入重投/死信流程。</p>
     *
     * <p>明确报错的情形：消息已确认（ALREADY_ACKED）、已进死信（DEAD_LETTERED）、
     * 从未投递给该订阅端或消息不存在（MESSAGE_NOT_FOUND）、续期时长非法（INVALID_EXTENSION）。</p>
     */
    public ExtendResult extend(String clientId, String messageId, Long extendMs) {
        if (messageId == null || messageId.isBlank()) {
            throw new InvalidSubscriptionException("extend.messageId 不能为空");
        }
        long ms = extendMs == null ? props.getAckTimeoutMs() : extendMs;
        if (ms <= 0 || ms > props.getMaxExtendMs()) {
            throw new ApiException("INVALID_EXTENSION",
                    "续期时长必须为正且不超过 broker.max-extend-ms=" + props.getMaxExtendMs()
                            + ": clientId=" + clientId + ", messageId=" + messageId
                            + ", extendMs=" + ms);
        }
        Subscription sub = subscriptions.get(clientId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "订阅端不存在: clientId=" + clientId + "，请先调用 /api/subscribe"));
        BrokerMessage msg = messages.find(messageId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "消息不存在: messageId=" + messageId));
        long seq = msg.globalSeq();
        synchronized (lock) {
            // 顺序敏感：在途 -> 已确认 -> 已死信 -> 从未投递给该订阅端（与 ACK 校验口径一致）
            PendingDelivery pd = sub.getPending(seq);
            if (pd != null) {
                long now = System.currentTimeMillis();
                pd.extend(now, ms);
                log.info("续期成功 clientId={} messageId={} seq={} 延长 {}ms，新到点={}",
                        clientId, messageId, seq, ms, pd.getExtendedUntil());
                return new ExtendResult(messageId, seq, pd.getExtendedUntil());
            }
            if (sub.isAcked(seq)) {
                throw new AlreadyAckedException(
                        "消息已经确认过，无需也无法续期: clientId=" + clientId
                                + ", messageId=" + messageId + ", seq=" + seq);
            }
            if (sub.isDead(seq)) {
                throw new ApiException("DEAD_LETTERED",
                        "消息已进入死信，不能续期；如需重新消费请走死信再投递: clientId=" + clientId
                                + ", messageId=" + messageId);
            }
            throw new MessageNotFoundException(
                    "该消息从未投递给此订阅端（可能主题不匹配或订阅晚于发布），无法续期: clientId="
                            + clientId + ", messageId=" + messageId + ", topic=" + msg.topic());
        }
    }

    // ------------------------------------------------------- 死信再投递

    /**
     * 死信再投递：把死信台账中尚未被消费掉的消息重新投给对应订阅端。
     *
     * <p>语义与边界：</p>
     * <ul>
     *   <li>整批先校验后执行（原子）：任一消息不存在/已确认/不在死信中，整批拒绝并说明原因；</li>
     *   <li>本批实际捞回的消息按全局序号（即发布先后）升序重新进入在途集合，
     *       下一次 poll 按既有"重投优先、序号升序"规则投出，相对顺序稳定；</li>
     *   <li>幂等：同一批重复触发时，已捞回的消息报告 ALREADY_REDELIVERED，
     *       不重复入队、不产生重复投递；</li>
     *   <li>容量：捞回后若在途总数会超过 broker.max-pending-per-subscription，
     *       整批拒绝（PENDING_FULL），绝不悄悄丢消息；</li>
     *   <li>捞回的消息开启新一轮投递周期（attempt 从 1 重计），messageId/seq 不变，
     *       消费进度游标不回退。</li>
     * </ul>
     */
    public RedeliverResult redeliverDeadLetters(String clientId, List<String> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) {
            throw new ApiException("INVALID_REQUEST",
                    "messageIds 不能为空：请至少指定一条要再投递的死信消息");
        }
        Subscription sub = subscriptions.get(clientId)
                .orElseThrow(() -> new MessageNotFoundException(
                        "订阅端不存在: clientId=" + clientId + "，请先调用 /api/subscribe"));
        long now = System.currentTimeMillis();
        synchronized (lock) {
            // 1) 逐条校验并分类（请求内重复的 messageId 只处理一次）
            Map<String, Long> seqById = new LinkedHashMap<>();
            Map<String, String> statusById = new LinkedHashMap<>();
            List<BrokerMessage> toRevive = new ArrayList<>();
            for (String id : messageIds) {
                if (id == null || id.isBlank()) {
                    throw new ApiException("INVALID_REQUEST",
                            "messageIds 中不允许出现空 messageId: clientId=" + clientId);
                }
                if (statusById.containsKey(id)) {
                    continue;
                }
                BrokerMessage msg = messages.find(id)
                        .orElseThrow(() -> new MessageNotFoundException(
                                "消息不存在，无法再投递: clientId=" + clientId + ", messageId=" + id));
                long seq = msg.globalSeq();
                seqById.put(id, seq);
                if (sub.isAcked(seq)) {
                    throw new AlreadyAckedException(
                            "消息已被该订阅端确认消费，不能从死信再投递: clientId=" + clientId
                                    + ", messageId=" + id + ", seq=" + seq);
                }
                Optional<DeadLetter> letter = dlq.find(clientId, id);
                if (letter.isEmpty()) {
                    if (sub.isPending(seq)) {
                        throw new ApiException("NOT_DEAD_LETTERED",
                                "消息正在投递途中，不在死信台账中，无需再投递: clientId=" + clientId
                                        + ", messageId=" + id + ", seq=" + seq);
                    }
                    throw new MessageNotFoundException(
                            "该消息从未投递给此订阅端，也不在死信台账中: clientId=" + clientId
                                    + ", messageId=" + id + ", topic=" + msg.topic());
                }
                if (letter.get().redelivered()) {
                    // 幂等：之前已捞回，本次不再重复入队
                    statusById.put(id, "ALREADY_REDELIVERED");
                } else {
                    statusById.put(id, "REDELIVERED");
                    toRevive.add(msg);
                }
            }
            // 2) 容量检查：宁可整批明确拒绝，也不悄悄丢消息
            int pendingAfter = sub.getPending().size() + toRevive.size();
            if (pendingAfter > props.getMaxPendingPerSubscription()) {
                throw new ApiException("PENDING_FULL",
                        "再投递后订阅端在途消息将达 " + pendingAfter
                                + "，超过上限 broker.max-pending-per-subscription="
                                + props.getMaxPendingPerSubscription()
                                + "（当前在途 " + sub.getPending().size() + "，本批待捞回 "
                                + toRevive.size() + "）；请先消费/确认部分在途消息或缩小批量后重试"
                                + ": clientId=" + clientId);
            }
            // 3) 按发布先后（全局序号升序）捞回，保证批内相对顺序稳定
            toRevive.sort(Comparator.comparingLong(BrokerMessage::globalSeq));
            for (BrokerMessage m : toRevive) {
                sub.revive(m, now);
                dlq.markRedelivered(clientId, m.id(), now);
            }
            // 4) 结果按请求顺序返回
            List<RedeliverItem> items = new ArrayList<>();
            int redelivered = 0;
            int already = 0;
            for (Map.Entry<String, String> e : statusById.entrySet()) {
                String status = e.getValue();
                if ("REDELIVERED".equals(status)) {
                    redelivered++;
                } else {
                    already++;
                }
                items.add(new RedeliverItem(e.getKey(), seqById.get(e.getKey()), status));
            }
            if (redelivered > 0 || already > 0) {
                log.info("死信再投递 clientId={} 本次捞回={} 已捞过(幂等跳过)={} 在途总数={}",
                        clientId, redelivered, already, sub.getPending().size());
            }
            return new RedeliverResult(clientId, redelivered, already, items);
        }
    }
}
