package com.github.feng111627.pubsub;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@DisplayName("REST API 场景：订阅/发布/拉取/确认/错误响应/死信查询")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"broker.ack-timeout=PT0.1S", "broker.max-delivery-attempts=2",
                "broker.redelivery-scan-interval=PT0.05S"})
class ApiIntegrationTest {

    @Autowired
    TestRestTemplate rest;

    @Test
    @DisplayName("场景：非法订阅过滤器返回 400 并说明原因")
    void invalidFilterReturns400() {
        ResponseEntity<Map> resp = rest.postForEntity("/broker/subscribe",
                Map.of("name", "bad", "filter", "a/#/b"), Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(resp.getBody().get("error")).isEqualTo("INVALID_TOPIC");
        assertThat((String) resp.getBody().get("message")).contains("'#' 只能出现在最后一段");
    }

    @Test
    @DisplayName("场景：发布-拉取-确认走通，重复确认返回 409，确认不存在返回 404")
    void publishPollAckAndErrorCases() {
        rest.postForEntity("/broker/subscribe",
                Map.of("name", "api-sub", "filter", "orders/+"), Map.class);
        Map published = rest.postForEntity("/broker/publish",
                Map.of("topic", "orders/created", "payload", "hello"), Map.class).getBody();
        assertThat(published.get("seq")).isNotNull();

        List<Map> polled = rest.postForEntity("/broker/subscriptions/api-sub/poll?max=5",
                null, List.class).getBody();
        assertThat(polled).hasSize(1);
        String deliveryId = (String) polled.get(0).get("deliveryId");

        ResponseEntity<Map> acked = rest.postForEntity("/broker/subscriptions/api-sub/ack",
                Map.of("deliveryId", deliveryId), Map.class);
        assertThat(acked.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<Map> dup = rest.postForEntity("/broker/subscriptions/api-sub/ack",
                Map.of("deliveryId", deliveryId), Map.class);
        assertThat(dup.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(dup.getBody().get("error")).isEqualTo("ALREADY_ACKED");

        ResponseEntity<Map> missing = rest.postForEntity("/broker/subscriptions/api-sub/ack",
                Map.of("deliveryId", "nope"), Map.class);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(missing.getBody().get("error")).isEqualTo("DELIVERY_NOT_FOUND");
    }

    @Test
    @DisplayName("场景：超时未确认被后台调度重投，超上限后落死信并可查询")
    void redeliveryAndDeadLetterViaApi() throws InterruptedException {
        rest.postForEntity("/broker/subscribe",
                Map.of("name", "dlq-sub", "filter", "#"), Map.class);
        rest.postForEntity("/broker/publish",
                Map.of("topic", "t/x", "payload", "doomed"), Map.class);

        // 第一次拉取但不确认，等后台调度重投
        List<Map> first = rest.postForEntity("/broker/subscriptions/dlq-sub/poll?max=1",
                null, List.class).getBody();
        assertThat(first).hasSize(1);

        List<Map> redelivered = null;
        for (int i = 0; i < 50; i++) { // 最多等 5 秒
            Thread.sleep(100);
            redelivered = rest.postForEntity("/broker/subscriptions/dlq-sub/poll?max=1",
                    null, List.class).getBody();
            if (!redelivered.isEmpty()) break;
        }
        assertThat(redelivered).as("超时后应被重投").isNotEmpty();
        assertThat(redelivered.get(0).get("attempt")).isEqualTo(2);

        // 第二次也不确认，超过 max-delivery-attempts=2 后应落死信
        List<Map> dead = null;
        for (int i = 0; i < 50; i++) {
            Thread.sleep(100);
            dead = rest.getForEntity("/broker/dead-letters?subscription=dlq-sub",
                    List.class).getBody();
            if (!dead.isEmpty()) break;
        }
        assertThat(dead).as("超过重投上限应落死信").isNotEmpty();
        assertThat(((Map) dead.get(0)).get("reason")).isEqualTo("MAX_DELIVERY_ATTEMPTS_EXCEEDED");
    }
}
