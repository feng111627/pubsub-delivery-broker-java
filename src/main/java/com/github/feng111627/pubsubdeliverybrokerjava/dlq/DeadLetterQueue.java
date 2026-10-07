package com.github.feng111627.pubsubdeliverybrokerjava.dlq;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 死信台账：所有"无法再靠重投完成"的消息都必须在此可查，绝不凭空消失。
 *
 * 进入死信的两种情形：
 * <ol>
 *   <li>{@code MAX_DELIVERIES_EXCEEDED}：ACK 超时且投递次数达到上限。</li>
 *   <li>{@code PENDING_OVERFLOW}：订阅端在途积压超过 {@code broker.max-pending-per-subscription}，
 *       按配置策略把最旧的未完成消息移出投递路径。</li>
 * </ol>
 */
@Component
public class DeadLetterQueue {

    private final List<DeadLetter> letters = new ArrayList<>();
    /** 已被再投递（redrive）捞回的死信：key -> 再投递时刻。用于台账展示与防止"越捞越多"。 */
    private final Map<String, Long> redrivenAt = new HashMap<>();

    public synchronized void record(DeadLetter letter) {
        letters.add(letter);
    }

    private static String key(String clientId, String messageId) {
        return clientId + " " + messageId;
    }

    /** 标记一条死信已被再投递回对应订阅端。 */
    public synchronized void markRedriven(String clientId, String messageId, long now) {
        redrivenAt.put(key(clientId, messageId), now);
    }

    public synchronized boolean isRedriven(String clientId, String messageId) {
        return redrivenAt.containsKey(key(clientId, messageId));
    }

    public synchronized Long redrivenAt(String clientId, String messageId) {
        return redrivenAt.get(key(clientId, messageId));
    }

    public synchronized List<DeadLetter> listByClient(String clientId) {
        List<DeadLetter> out = new ArrayList<>();
        for (DeadLetter l : letters) {
            if (l.clientId().equals(clientId)) {
                out.add(l);
            }
        }
        return out;
    }

    public synchronized List<DeadLetter> listAll() {
        return new ArrayList<>(letters);
    }

    public synchronized int size() {
        return letters.size();
    }
}
