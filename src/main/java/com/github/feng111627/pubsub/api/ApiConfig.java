package com.github.feng111627.pubsub.api;

import com.github.feng111627.pubsub.broker.BrokerProperties;
import com.github.feng111627.pubsub.broker.DeadLetterStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(BrokerProperties.class)
public class ApiConfig {

    @Bean
    public DeadLetterStore deadLetterStore() {
        return new DeadLetterStore();
    }
}
