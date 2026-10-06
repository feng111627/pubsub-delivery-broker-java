package com.github.feng111627.pubsubdeliverybrokerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Broker 可调参数（骨架：仅类型定义与默认值）。
 */
@ConfigurationProperties(prefix = "broker")
public class BrokerProperties {

    /** 投递后等待 ACK 的超时时间（毫秒），超时触发重投 */
    private int ackTimeoutMs = 10_000;

    /** 最大投递（重投）次数，超过后进入死信 */
    private int maxDeliveries = 3;

    /** 单订阅端在途（未确认）消息上限 */
    private int maxPendingPerSubscription = 100;

    /** 积压达到上限时的策略：dead-letter（默认）/ drop-oldest */
    private String overflowStrategy = "dead-letter";

    /** 重投扫描间隔（毫秒） */
    private int redeliveryScanMs = 200;

    public int getAckTimeoutMs() { return ackTimeoutMs; }
    public void setAckTimeoutMs(int ackTimeoutMs) { this.ackTimeoutMs = ackTimeoutMs; }
    public int getMaxDeliveries() { return maxDeliveries; }
    public void setMaxDeliveries(int maxDeliveries) { this.maxDeliveries = maxDeliveries; }
    public int getMaxPendingPerSubscription() { return maxPendingPerSubscription; }
    public void setMaxPendingPerSubscription(int maxPendingPerSubscription) { this.maxPendingPerSubscription = maxPendingPerSubscription; }
    public String getOverflowStrategy() { return overflowStrategy; }
    public void setOverflowStrategy(String overflowStrategy) { this.overflowStrategy = overflowStrategy; }
    public int getRedeliveryScanMs() { return redeliveryScanMs; }
    public void setRedeliveryScanMs(int redeliveryScanMs) { this.redeliveryScanMs = redeliveryScanMs; }
}
