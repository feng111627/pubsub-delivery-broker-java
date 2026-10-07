package com.github.feng111627.pubsubdeliverybrokerjava.dlq;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 死信台账：所有"无法再靠重投完成"的消息都必须在此可查，绝不凭空消失。
 *
 * 进入死信的两种情形：
 * <ol>
 *   <li>{@code MAX_DELIVERIES_EXCEEDED}：ACK 超时且投递次数达到上限。</li>
 *   <li>{@code PENDING_OVERFLOW}：订阅端在途积压超过 {@code broker.max-pending-per-subscription}，
 *       按配置策略把最旧的未完成消息移出投递路径。</li>
 * </ol>
 *
 * <p>台账按 (clientId, messageId) 去重：同一条消息对同一订阅端只占一条记录。
 * 消息被再投递捞回后若再次死信，记录会被刷新（新的 deadAt/reason，redeliveredAt 归零），
 * 因此查询结果不会因为反复打捞而越捞越多。</p>
 */
@Component
public class DeadLetterQueue {

    /** key = clientId + messageId，保持插入顺序，查询输出稳定。 */
    private final Map<String, DeadLetter> letters = new LinkedHashMap<>();

    private static String key(String clientId, String messageId) {
        return clientId + " " + messageId;
    }

    public synchronized void record(DeadLetter letter) {
        letters.put(key(letter.clientId(), letter.messageId()), letter);
    }

    /** 查找某订阅端某消息的死信记录（无论是否已被再投递）。 */
    public synchronized Optional<DeadLetter> find(String clientId, String messageId) {
        return Optional.ofNullable(letters.get(key(clientId, messageId)));
    }

    /**
     * 把一条死信标记为"已再投递"。调用方需先确认记录存在且未再投递过。
     */
    public synchronized DeadLetter markRedelivered(String clientId, String messageId, long now) {
        String k = key(clientId, messageId);
        DeadLetter old = letters.get(k);
        DeadLetter updated = new DeadLetter(old.clientId(), old.messageId(), old.topic(),
                old.reason(), old.deadAt(), now);
        letters.put(k, updated);
        return updated;
    }

    public synchronized List<DeadLetter> listByClient(String clientId) {
        List<DeadLetter> out = new ArrayList<>();
        for (DeadLetter l : letters.values()) {
            if (l.clientId().equals(clientId)) {
                out.add(l);
            }
        }
        return out;
    }

    public synchronized List<DeadLetter> listAll() {
        return new ArrayList<>(letters.values());
    }

    public synchronized int size() {
        return letters.size();
    }
}
