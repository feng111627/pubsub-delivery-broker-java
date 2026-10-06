package com.github.feng111627.pubsub.broker;

public enum DeliveryStatus {
    PENDING,    // 待投递
    IN_FLIGHT,  // 已投递待确认
    ACKED,      // 已确认
    DEAD        // 超过重投上限，进入死信
}
