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
                    // 客户端尚未拉取过的消息不参与超时，避免"未投递先重投"
                    if (!pd.isDelivered() || now - pd.getLastSentAt() < timeout) {
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
}
