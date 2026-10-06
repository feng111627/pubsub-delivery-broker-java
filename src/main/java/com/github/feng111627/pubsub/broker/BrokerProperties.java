package com.github.feng111627.pubsub.broker;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** 投递相关配置项。 */
@ConfigurationProperties(prefix = "broker")
public class BrokerProperties {

    /** 确认超时，超时未 ack 的投递会被重投。 */
    private Duration ackTimeout = Duration.ofSeconds(30);
    /** 最大投递次数（含首次），超过后进入死信。 */
    private int maxDeliveryAttempts = 5;
    /** 单订阅端最大积压（pending + in-flight），达到上限触发积压策略。 */
    private int maxBacklog = 1000;
    /** 积压策略：DEAD_LETTER（溢出消息落死信）或 REJECT（拒绝发布）。 */
    private BacklogStrategy backlogStrategy = BacklogStrategy.DEAD_LETTER;
    /** 重投扫描间隔。 */
    private Duration redeliveryScanInterval = Duration.ofMillis(500);

    public enum BacklogStrategy { DEAD_LETTER, REJECT }

    public Duration getAckTimeout() { return ackTimeout; }
    public void setAckTimeout(Duration ackTimeout) { this.ackTimeout = ackTimeout; }
    public int getMaxDeliveryAttempts() { return maxDeliveryAttempts; }
    public void setMaxDeliveryAttempts(int maxDeliveryAttempts) { this.maxDeliveryAttempts = maxDeliveryAttempts; }
    public int getMaxBacklog() { return maxBacklog; }
    public void setMaxBacklog(int maxBacklog) { this.maxBacklog = maxBacklog; }
    public BacklogStrategy getBacklogStrategy() { return backlogStrategy; }
    public void setBacklogStrategy(BacklogStrategy backlogStrategy) { this.backlogStrategy = backlogStrategy; }
    public Duration getRedeliveryScanInterval() { return redeliveryScanInterval; }
    public void setRedeliveryScanInterval(Duration redeliveryScanInterval) { this.redeliveryScanInterval = redeliveryScanInterval; }
}
