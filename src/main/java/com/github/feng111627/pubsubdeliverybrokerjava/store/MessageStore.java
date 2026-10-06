package com.github.feng111627.pubsubdeliverybrokerjava.store;

import com.github.feng111627.pubsubdeliverybrokerjava.model.BrokerMessage;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存消息存储：维护全局单调序号与每个主题内的单调序号。
 *
 * <p>设计约定：{@link #append} 由 BrokerService 在持有 Broker 全局锁时调用，
 * 因此序号分配天然串行；其余读方法无副作用，锁内外均可调用。</p>
 */
@Component
public class MessageStore {

    /** 消息按 globalSeq 升序存放，下标即 globalSeq-1 */
    private final List<BrokerMessage> messages = new ArrayList<>();
    private final AtomicLong globalSeq = new AtomicLong(0);
    private final Map<String, Long> topicSeq = new ConcurrentHashMap<>();

    /**
     * 追加一条消息，分配全局序号与主题内序号。
     */
    public BrokerMessage append(String topic, String payload) {
        long gseq = globalSeq.incrementAndGet();
        long tseq = topicSeq.merge(topic, 1L, Long::sum);
        BrokerMessage msg = new BrokerMessage(
                "msg-" + gseq + "-" + Long.toHexString(((long) topic.hashCode() << 32) ^ tseq),
                topic, payload, System.currentTimeMillis(), gseq, tseq);
        messages.add(msg);
        return msg;
    }

    public Optional<BrokerMessage> find(String messageId) {
        for (BrokerMessage m : messages) {
            if (m.id().equals(messageId)) {
                return Optional.of(m);
            }
        }
        return Optional.empty();
    }

    /**
     * 返回全局序号位于 {@code (fromSeq, toSeq]} 区间的消息，按全局序号升序。
     * 这正是"上次投递位置之后、当前最新位置之前"需要投递给订阅端的区间。
     */
    public List<BrokerMessage> range(long fromSeq, long toSeq) {
        List<BrokerMessage> out = new ArrayList<>();
        if (toSeq <= fromSeq) {
            return out;
        }
        int fromIndex = (int) Math.max(0, fromSeq);
        int toIndex = (int) Math.min(messages.size(), toSeq);
        for (int i = fromIndex; i < toIndex; i++) {
            out.add(messages.get(i));
        }
        return out;
    }

    public long currentGlobalSeq() {
        return globalSeq.get();
    }
}
