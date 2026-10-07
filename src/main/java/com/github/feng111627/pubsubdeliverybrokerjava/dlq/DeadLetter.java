package com.github.feng111627.pubsubdeliverybrokerjava.dlq;

/**
 * 死信记录。
 *
 * @param redeliveredAt 该死信被"再投递"捞回的时间戳（毫秒），0 表示尚未再投递。
 *                      借此可以区分"还躺在死信里"与"已经捞回重新投递"的消息，
 *                      避免重复打捞导致越捞越多。
 */
public record DeadLetter(String clientId, String messageId, String topic,
                         String reason, long deadAt, long redeliveredAt) {

    /** 是否已被再投递捞回。 */
    public boolean redelivered() {
        return redeliveredAt > 0;
    }
}
