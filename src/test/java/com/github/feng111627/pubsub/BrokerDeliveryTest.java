package com.github.feng111627.pubsub;

import com.github.feng111627.pubsub.broker.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

@DisplayName("投递语义：至少一次、确认、重投、死信、重连续传、隔离与顺序")
class BrokerDeliveryTest {

    private BrokerProperties props;
    private DeadLetterStore deadLetterStore;
    private Broker broker;

    @BeforeEach
    void setUp() {
        props = new BrokerProperties();
        props.setAckTimeout(Duration.ofMillis(80));
        props.setMaxDeliveryAttempts(3);
        props.setMaxBacklog(10);
        deadLetterStore = new DeadLetterStore();
        broker = new Broker(props, deadLetterStore);
    }

    @Test
    @DisplayName("主流程：发布->拉取->确认后不再投递")
    void publishPollAckCompletes() {
        broker.subscribe("s1", "orders/+");
        broker.publish("orders/created", "hello");

        List<Delivery> pulled = broker.poll("s1", 10);
        assertThat(pulled).hasSize(1);
        assertThat(pulled.get(0).message().payload()).isEqualTo("hello");
        assertThat(pulled.get(0).attempts()).isEqualTo(1);

        broker.ack("s1", pulled.get(0).id());
        assertThat(broker.poll("s1", 10)).isEmpty();
        assertThat(broker.getSubscription("s1").ackedSeqOf("orders/created")).isEqualTo(1);
    }

    @Test
    @DisplayName("不匹配的主题不会投递给订阅端")
    void nonMatchingTopicNotDelivered() {
        broker.subscribe("s1", "orders/+");
        broker.publish("payments/created", "x");
        assertThat(broker.poll("s1", 10)).isEmpty();
    }

    @Test
    @DisplayName("确认不存在的投递：明确报 DELIVERY_NOT_FOUND")
    void ackUnknownDeliveryFails() {
        broker.subscribe("s1", "#");
        assertThatThrownBy(() -> broker.ack("s1", "no-such-delivery"))
                .isInstanceOf(BrokerException.class)
                .extracting(e -> ((BrokerException) e).code())
                .isEqualTo(BrokerException.Code.DELIVERY_NOT_FOUND);
    }

    @Test
    @DisplayName("重复确认同一条投递：明确报 ALREADY_ACKED")
    void duplicateAckFails() {
        broker.subscribe("s1", "#");
        broker.publish("t/a", "x");
        Delivery d = broker.poll("s1", 1).get(0);
        broker.ack("s1", d.id());
        assertThatThrownBy(() -> broker.ack("s1", d.id()))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("已确认过")
                .extracting(e -> ((BrokerException) e).code())
                .isEqualTo(BrokerException.Code.ALREADY_ACKED);
    }

    @Test
    @DisplayName("对不存在的订阅端拉取/确认：明确报 SUBSCRIPTION_NOT_FOUND")
    void unknownSubscriptionFails() {
        assertThatThrownBy(() -> broker.poll("ghost", 1))
                .isInstanceOf(BrokerException.class)
                .extracting(e -> ((BrokerException) e).code())
                .isEqualTo(BrokerException.Code.SUBSCRIPTION_NOT_FOUND);
        assertThatThrownBy(() -> broker.ack("ghost", "d1"))
                .isInstanceOf(BrokerException.class)
                .extracting(e -> ((BrokerException) e).code())
                .isEqualTo(BrokerException.Code.SUBSCRIPTION_NOT_FOUND);
    }

    @Test
    @DisplayName("订阅过滤器非法：明确报 INVALID_TOPIC 并说明原因")
    void invalidFilterRejected() {
        assertThatThrownBy(() -> broker.subscribe("s1", "a/#/b"))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("'#' 只能出现在最后一段");
    }

    @Test
    @DisplayName("同名订阅换过滤器：明确拒绝")
    void conflictingFilterRejected() {
        broker.subscribe("s1", "orders/#");
        assertThatThrownBy(() -> broker.subscribe("s1", "payments/#"))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("不一致");
    }

    @Test
    @DisplayName("超时未确认：消息被重投（至少一次）")
    void ackTimeoutTriggersRedelivery() throws InterruptedException {
        broker.subscribe("s1", "#");
        broker.publish("t/a", "x");
        Delivery first = broker.poll("s1", 1).get(0);

        Thread.sleep(120); // 超过 ackTimeout=80ms
        assertThat(broker.redeliverExpired()).isEqualTo(1);

        List<Delivery> again = broker.poll("s1", 10);
        assertThat(again).hasSize(1);
        assertThat(again.get(0).message().id()).isEqualTo(first.message().id());
        assertThat(again.get(0).attempts()).isEqualTo(2);
    }

    @Test
    @DisplayName("重投超过上限：消息落死信且可查询，不凭空消失")
    void maxAttemptsMovesToDeadLetter() throws InterruptedException {
        broker.subscribe("s1", "#");
        broker.publish("t/a", "doomed");

        for (int i = 0; i < 3; i++) { // maxDeliveryAttempts=3
            List<Delivery> pulled = broker.poll("s1", 10);
            assertThat(pulled).hasSize(1);
            Thread.sleep(120);
            broker.redeliverExpired();
        }

        assertThat(broker.poll("s1", 10)).isEmpty();
        List<DeadLetterStore.DeadLetter> dead = deadLetterStore.listBySubscription("s1");
        assertThat(dead).hasSize(1);
        assertThat(dead.get(0).reason()).isEqualTo("MAX_DELIVERY_ATTEMPTS_EXCEEDED");
        assertThat(dead.get(0).message().payload()).isEqualTo("doomed");
        assertThat(dead.get(0).attempts()).isEqualTo(3);
    }

    @Test
    @DisplayName("断线重连：未确认的投递重新可拉取，进度接着上次继续")
    void reconnectResumesFromCursor() {
        broker.subscribe("s1", "#");
        broker.publish("t/a", "m1");
        broker.publish("t/a", "m2");
        broker.publish("t/a", "m3");

        List<Delivery> first = broker.poll("s1", 3);
        broker.ack("s1", first.get(0).id()); // 只确认 m1 就“断线”

        broker.subscribe("s1", "#"); // 重连：同名同过滤器
        List<Delivery> resumed = broker.poll("s1", 10);
        assertThat(resumed).hasSize(2);
        assertThat(resumed.stream().map(d -> d.message().payload()))
                .containsExactly("m2", "m3");
        // 进度不回退：m1 的确认进度还在
        assertThat(broker.getSubscription("s1").ackedSeqOf("t/a")).isEqualTo(1);
    }

    @Test
    @DisplayName("迟到的旧确认不会把进度拉回去")
    void lateAckOfOldMessageDoesNotRegressCursor() {
        broker.subscribe("s1", "#");
        broker.publish("t/a", "m1");
        broker.publish("t/a", "m2");
        List<Delivery> pulled = broker.poll("s1", 2);

        broker.ack("s1", pulled.get(1).id()); // 先确认 seq=2
        broker.ack("s1", pulled.get(0).id()); // 后确认 seq=1（迟到）
        assertThat(broker.getSubscription("s1").ackedSeqOf("t/a")).isEqualTo(2);
    }

    @Test
    @DisplayName("慢订阅端积压不拖累其他订阅端")
    void slowSubscriberDoesNotBlockOthers() {
        broker.subscribe("slow", "#");
        broker.subscribe("fast", "#");
        List<Delivery> fastDeliveries = new ArrayList<>();
        for (int i = 0; i < 20; i++) { // 超过 slow 的积压上限 10
            broker.publish("t/a", "m" + i);
            List<Delivery> batch = broker.poll("fast", 10); // fast 正常消费并确认
            batch.forEach(d -> broker.ack("fast", d.id()));
            fastDeliveries.addAll(batch);
        }
        // slow 从不拉取：积压到上限 10，溢出部分落死信
        assertThat(broker.poll("slow", 100)).hasSize(10);
        assertThat(deadLetterStore.listBySubscription("slow")).hasSize(10);
        // fast 不受 slow 影响，收到全部 20 条且顺序稳定
        assertThat(fastDeliveries).hasSize(20);
        assertThat(fastDeliveries.stream().mapToLong(d -> d.message().seq()))
                .isSorted();
    }

    @Test
    @DisplayName("积压策略=REJECT：订阅端积压满时发布被明确拒绝")
    void backlogRejectStrategy() {
        props.setBacklogStrategy(BrokerProperties.BacklogStrategy.REJECT);
        broker.subscribe("s1", "#");
        for (int i = 0; i < 10; i++) {
            broker.publish("t/a", "m" + i);
        }
        assertThatThrownBy(() -> broker.publish("t/a", "overflow"))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("积压已达上限")
                .extracting(e -> ((BrokerException) e).code())
                .isEqualTo(BrokerException.Code.BACKLOG_FULL);
    }

    @Test
    @DisplayName("高并发发布同一主题：序号唯一且订阅端按序收到")
    void concurrentPublishKeepsStableOrder() throws InterruptedException {
        props.setMaxBacklog(10000); // 本场景只验证顺序，不触发积压策略
        broker.subscribe("s1", "t/a");
        int threads = 8, perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            int base = t * perThread;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        broker.publish("t/a", "m" + (base + i));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        List<Delivery> all = new ArrayList<>();
        List<Delivery> batch;
        while (!(batch = broker.poll("s1", 100)).isEmpty()) {
            all.addAll(batch);
        }
        assertThat(all).hasSize(threads * perThread);
        List<Long> seqs = all.stream().map(d -> d.message().seq()).toList();
        assertThat(seqs).isSorted();                       // 顺序稳定
        assertThat(seqs).containsExactlyElementsOf(        // 序号恰好是 1..N，无重无漏
                java.util.stream.LongStream.rangeClosed(1, threads * perThread).boxed().toList());
    }
}
