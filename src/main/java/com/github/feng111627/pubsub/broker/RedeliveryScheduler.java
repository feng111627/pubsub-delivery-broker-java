package com.github.feng111627.pubsub.broker;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 周期性触发超时重投扫描。 */
@Component
public class RedeliveryScheduler {

    private final Broker broker;

    public RedeliveryScheduler(Broker broker) {
        this.broker = broker;
    }

    @Scheduled(fixedDelayString = "${broker.redelivery-scan-interval:PT0.5S}")
    public void scan() {
        broker.redeliverExpired();
    }
}
