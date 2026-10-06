package com.github.feng111627.pubsub.api;

import com.github.feng111627.pubsub.broker.Broker;
import com.github.feng111627.pubsub.broker.DeadLetterStore;
import com.github.feng111627.pubsub.broker.Delivery;
import com.github.feng111627.pubsub.broker.Message;
import com.github.feng111627.pubsub.broker.Subscription;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/broker")
public class BrokerController {

    private final Broker broker;
    private final DeadLetterStore deadLetterStore;

    public BrokerController(Broker broker, DeadLetterStore deadLetterStore) {
        this.broker = broker;
        this.deadLetterStore = deadLetterStore;
    }

    public record PublishRequest(String topic, String payload) {}
    public record SubscribeRequest(String name, String filter) {}
    public record AckRequest(String deliveryId) {}

    @PostMapping("/publish")
    public Map<String, Object> publish(@RequestBody PublishRequest req) {
        Message m = broker.publish(req.topic(), req.payload());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("messageId", m.id());
        body.put("topic", m.topic().toString());
        body.put("seq", m.seq());
        return body;
    }

    @PostMapping("/subscribe")
    public Map<String, Object> subscribe(@RequestBody SubscribeRequest req) {
        Subscription s = broker.subscribe(req.name(), req.filter());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("subscription", s.name());
        body.put("filter", s.filter().toString());
        return body;
    }

    @PostMapping("/subscriptions/{name}/poll")
    public List<Map<String, Object>> poll(@PathVariable String name,
                                          @RequestParam(defaultValue = "10") int max) {
        return broker.poll(name, max).stream().map(BrokerController::toJson).toList();
    }

    @PostMapping("/subscriptions/{name}/ack")
    public Map<String, Object> ack(@PathVariable String name, @RequestBody AckRequest req) {
        broker.ack(name, req.deliveryId());
        return Map.of("acked", req.deliveryId());
    }

    @GetMapping("/dead-letters")
    public List<DeadLetterStore.DeadLetter> deadLetters(@RequestParam(required = false) String subscription) {
        return subscription == null ? deadLetterStore.list() : deadLetterStore.listBySubscription(subscription);
    }

    private static Map<String, Object> toJson(Delivery d) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("deliveryId", d.id());
        body.put("messageId", d.message().id());
        body.put("topic", d.message().topic().toString());
        body.put("seq", d.message().seq());
        body.put("payload", d.message().payload());
        body.put("attempt", d.attempts());
        body.put("ackDeadline", d.ackDeadline().toString());
        return body;
    }
}
