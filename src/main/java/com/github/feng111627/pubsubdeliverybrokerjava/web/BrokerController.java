package com.github.feng111627.pubsubdeliverybrokerjava.web;

import com.github.feng111627.pubsubdeliverybrokerjava.dlq.DeadLetter;
import com.github.feng111627.pubsubdeliverybrokerjava.service.BrokerService;
import com.github.feng111627.pubsubdeliverybrokerjava.web.dto.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/** REST 接口（骨架）。 */
@RestController
@RequestMapping("/api")
public class BrokerController {

    private final BrokerService brokerService;

    public BrokerController(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @PostMapping("/publish")
    public ResponseEntity<PublishResult> publish(@RequestBody PublishRequest request) {
        return ResponseEntity.ok(brokerService.publish(request));
    }

    @PostMapping("/subscribe")
    public ResponseEntity<SubscribeResponse> subscribe(@RequestBody SubscribeRequest request) {
        return ResponseEntity.ok(brokerService.subscribe(request.clientId(), request.filter()));
    }

    @PostMapping("/subscriptions/{clientId}/poll")
    public ResponseEntity<List<DeliveryView>> poll(@PathVariable String clientId,
                                                   @RequestParam(defaultValue = "32") int max) {
        return ResponseEntity.ok(brokerService.poll(clientId, max));
    }

    @PostMapping("/subscriptions/{clientId}/ack")
    public ResponseEntity<Void> ack(@PathVariable String clientId,
                                    @RequestBody AckRequest request) {
        brokerService.acknowledge(clientId, request.messageId());
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/dead-letters")
    public ResponseEntity<List<DeadLetter>> deadLetters(@RequestParam(required = false) String clientId) {
        return ResponseEntity.ok(brokerService.deadLetters(clientId));
    }
}
