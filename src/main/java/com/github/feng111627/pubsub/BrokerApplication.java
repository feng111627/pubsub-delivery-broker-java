package com.github.feng111627.pubsub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class BrokerApplication {
    public static void main(String[] args) {
        SpringApplication.run(BrokerApplication.class, args);
    }
}
