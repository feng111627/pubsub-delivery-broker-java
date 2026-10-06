package com.github.feng111627.pubsubdeliverybrokerjava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 周期性扫描 ACK 超时的在途消息：触发重投或转死信。 */
@Component
public class RedeliveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RedeliveryScheduler.class);

    private final BrokerService brokerService;

    public RedeliveryScheduler(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Scheduled(fixedDelayString = "${broker.redelivery-scan-ms:200}")
    public void tick() {
        if (brokerService == null) {
            return;
        }
        int processed = brokerService.redeliverTimedOut();
        if (processed > 0) {
            log.debug("重投扫描处理 {} 条在途消息", processed);
        }
    }
}
