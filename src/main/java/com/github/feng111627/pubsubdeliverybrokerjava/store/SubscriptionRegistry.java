package com.github.feng111627.pubsubdeliverybrokerjava.store;

import com.github.feng111627.pubsubdeliverybrokerjava.model.Subscription;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * clientId -> Subscription 注册表。
 *
 * <p>clientId 标识"同一个订阅端"：断线重连携带同一 clientId，即复用原订阅与投递状态，
 * 因而能从上次位置继续。实际的创建/冲突判断由 BrokerService 在全局锁内完成。</p>
 */
@Component
public class SubscriptionRegistry {

    private final Map<String, Subscription> byClient = new ConcurrentHashMap<>();

    /** 不存在则登记并返回新对象；已存在则原样返回（filter 冲突由调用方判断）。 */
    public Subscription getOrCreate(String clientId, String filter, long startSeq, long now) {
        return byClient.computeIfAbsent(clientId,
                id -> new Subscription(id, filter, startSeq, now));
    }

    public Optional<Subscription> get(String clientId) {
        return Optional.ofNullable(byClient.get(clientId));
    }

    public List<Subscription> all() {
        return new ArrayList<>(byClient.values());
    }
}
