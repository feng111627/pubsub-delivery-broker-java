package com.github.feng111627.pubsubdeliverybrokerjava.tests;

import com.github.feng111627.pubsubdeliverybrokerjava.config.BrokerProperties;
import com.github.feng111627.pubsubdeliverybrokerjava.dlq.DeadLetter;
import com.github.feng111627.pubsubdeliverybrokerjava.dlq.DeadLetterQueue;
import com.github.feng111627.pubsubdeliverybrokerjava.exception.AlreadyAckedException;
import com.github.feng111627.pubsubdeliverybrokerjava.exception.ApiException;
import com.github.feng111627.pubsubdeliverybrokerjava.exception.MessageNotFoundException;
import com.github.feng111627.pubsubdeliverybrokerjava.service.BrokerService;
import com.github.feng111627.pubsubdeliverybrokerjava.store.MessageStore;
import com.github.feng111627.pubsubdeliverybrokerjava.store.SubscriptionRegistry;
import com.github.feng111627.pubsubdeliverybrokerjava.web.dto.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景驱动的 Broker 核心语义测试。日志中以"场景-XX"开头，可直观看出覆盖的业务场景与用例结果。
 * 这里手工构造 Broker 并直接调用 redeliverTimedOut() 驱动超时，不依赖真实调度器，时序完全确定。
 */
class BrokerServiceTest {

    private BrokerService broker;
    private BrokerProperties props;

    private BrokerService brokerWith(BrokerProperties p) {
        return new BrokerService(new MessageStore(), new SubscriptionRegistry(),
                new DeadLetterQueue(), p);
    }

    @BeforeEach
    void setUp() {
        props = new BrokerProperties();
        broker = brokerWith(props);
    }

    private PublishResult publish(String topic, String payload) {
        return broker.publish(new PublishRequest(topic, payload));
    }

    private void subscribe(String clientId, String filter) {
        broker.subscribe(clientId, filter);
    }

    private DeliveryView pollOne(String clientId) {
        List<DeliveryView> views = broker.poll(clientId, 10);
        return views.isEmpty() ? null : views.get(0);
    }

    private List<DeliveryView> pollAll(String clientId) {
        return broker.poll(clientId, 1000);
    }

    private void ack(String clientId, String messageId) {
        broker.acknowledge(clientId, messageId);
    }

    // ------------------------------------------------------- 场景一：过滤与订阅校验

    @Nested
    @DisplayName("场景一：主题过滤、非法订阅拒绝")
    class Filtering {

        @Test
        @DisplayName("只有匹配 filter 的主题才投递；不同订阅端各自过滤")
        void filterRouting() {
            Logger log = LoggerFactory.getLogger("场景一-过滤");
            log.info("[用例] a/# 收到 a/b、a；c 不收；# 全收");
            subscribe("sub-a", "a/#");
            subscribe("sub-all", "#");
            publish("a/b", "1");
            publish("a", "2");
            publish("c", "3");

            List<DeliveryView> a = pollAll("sub-a");
            List<DeliveryView> all = pollAll("sub-all");
            assertEquals(List.of("1", "2"), payloads(a));
            assertEquals(List.of("1", "2", "3"), payloads(all));
        }

        @Test
        @DisplayName("非法 filter 被明确拒绝并说明问题；同一 clientId 换 filter 报冲突")
        void invalidAndConflict() {
            Logger log = LoggerFactory.getLogger("场景一-过滤");
            log.info("[用例] 非法 filter 报 INVALID_SUBSCRIPTION；重连换 filter 报 SUBSCRIPTION_CONFLICT(409)");
            ApiException bad = assertThrows(ApiException.class,
                    () -> subscribe("c1", "a//b"));
            assertEquals("INVALID_SUBSCRIPTION", bad.getErrorCode());
            assertTrue(bad.getMessage().contains("空层级"));

            subscribe("c1", "a/+");
            ApiException conflict = assertThrows(ApiException.class,
                    () -> subscribe("c1", "b/+"));
            assertEquals("SUBSCRIPTION_CONFLICT", conflict.getErrorCode());
            assertTrue(conflict.getMessage().contains("a/+"));
        }

        @Test
        @DisplayName("不存在的 clientId 轮询/确认报错")
        void unknownClient() {
            Logger log = LoggerFactory.getLogger("场景一-过滤");
            log.info("[用例] 未订阅直接 poll/ack 返回 MESSAGE_NOT_FOUND");
            publish("a", "x");
            assertThrows(MessageNotFoundException.class, () -> pollAll("ghost"));
            assertThrows(MessageNotFoundException.class, () -> ack("ghost", "msg-1-x"));
        }
    }

    // ------------------------------------------------- 场景二：重连续传 + 顺序稳定

    @Nested
    @DisplayName("场景二：断线重连续传、进度不回退、同主题顺序稳定")
    class ResumeAndOrdering {

        @Test
        @DisplayName("未 ACK 断线重连：同 clientId 再连，重投同一批消息且序号不回退")
        void resumeRedeliversUnacked() {
            Logger log = LoggerFactory.getLogger("场景二-重连续传");
            subscribe("dev", "orders/#");
            PublishResult m1 = publish("orders/1", "first");
            publish("orders/2", "second");

            List<DeliveryView> firstBatch = pollAll("dev");
            assertEquals(2, firstBatch.size());
            ack("dev", m1.messageId());
            log.info("[用例] 仅 ACK 第一条后模拟断线，未 ACK 的第二条必须重投且 attempt>1");

            // 模拟断线后超时：第二条会被标记重投
            DeliveryView second = firstBatch.get(1);
            // 未超时前扫描不产生重投
            assertEquals(0, broker.redeliverTimedOut());
            // 直接用一个超时配置很小的 broker 不方便（状态在内），这里用反射改配置模拟等待
            props.setAckTimeoutMs(-1);
            broker.redeliverTimedOut();

            List<DeliveryView> afterReconnect = pollAll("dev");
            List<DeliveryView> redeliveries = afterReconnect.stream()
                    .filter(DeliveryView::redelivery).toList();
            assertEquals(1, redeliveries.size());
            assertEquals(second.messageId(), redeliveries.get(0).messageId());
            assertEquals(2, redeliveries.get(0).deliveryAttempt());
            assertEquals(second.seq(), redeliveries.get(0).seq(), "重投序号必须与首次一致，进度不回退");
            log.info("[用例结果] 重投消息 seq={} attempt=2，第一条已确认不再重投",
                    redeliveries.get(0).seq());
        }

        @Test
        @DisplayName("新订阅只收订阅之后的消息；重连同 clientId 从原位置继续")
        void startAtCurrentPosition() {
            Logger log = LoggerFactory.getLogger("场景二-重连续传");
            PublishResult before = publish("t", "old");
            log.info("[用例] 订阅前的消息 {} 不应收到", before.messageId());
            subscribe("late", "t");
            PublishResult after = publish("t", "new");

            List<DeliveryView> got = pollAll("late");
            assertEquals(List.of("new"), payloads(got));
            assertEquals(after.globalSeq(), got.get(0).seq());

            // 重连（同 filter）返回的位置仍是原来的已投递位置，不重置
            SubscribeResponse resp = broker.subscribe("late", "t");
            assertEquals(2, resp.position(), "重连返回的 position 不回退（已投递到 seq=2）");
        }

        @Test
        @DisplayName("同一主题高并发发布：每个订阅端按 topicSeq/globalSeq 稳定升序收到")
        void concurrentPublishOrdering() throws Exception {
            Logger log = LoggerFactory.getLogger("场景二-重连续传");
            props.setMaxPendingPerSubscription(100_000);
            subscribe("s", "hot/#");
            int threads = 8;
            int perThread = 100;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            log.info("[用例] {} 线程各发布 {} 条同主题消息，共 {} 条", threads, perThread,
                    threads * perThread);
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            publish("hot/topic", "m");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            pool.shutdown();

            List<DeliveryView> got = pollAll("s");
            assertEquals(threads * perThread, got.size(), "不能丢消息");
            List<Long> seqs = got.stream().map(DeliveryView::seq).collect(Collectors.toList());
            List<Long> sorted = new ArrayList<>(seqs);
            java.util.Collections.sort(sorted);
            assertEquals(sorted, seqs, "投递顺序必须严格按序号升序");
            assertEquals(seqs.size(), Set.copyOf(seqs).size(), "不能重复投递");
            log.info("[用例结果] 共收到 {} 条，序号严格升序、无重复、无缺失", got.size());
        }
    }

    // ------------------------------------------------- 场景三：至少一次与 ACK 边界

    @Nested
    @DisplayName("场景三：至少一次投递、ACK 校验、超时重投、死信")
    class DeliveryGuarantees {

        @Test
        @DisplayName("重复 ACK 已确认消息 -> ALREADY_ACKED；ACK 不存在消息 -> MESSAGE_NOT_FOUND")
        void ackEdgeCases() {
            Logger log = LoggerFactory.getLogger("场景三-确认边界");
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            DeliveryView d = pollOne("c");

            ack("c", m.messageId());
            log.info("[用例] 首次 ACK 成功；第二次 ACK 必须明确报错而非静默成功");
            AlreadyAckedException dup = assertThrows(AlreadyAckedException.class,
                    () -> ack("c", m.messageId()));
            assertEquals("ALREADY_ACKED", dup.getErrorCode());
            assertTrue(dup.getMessage().contains(m.messageId()));

            MessageNotFoundException missing = assertThrows(MessageNotFoundException.class,
                    () -> ack("c", "msg-does-not-exist"));
            assertEquals("MESSAGE_NOT_FOUND", missing.getErrorCode());
            log.info("[用例] 不存在消息与主题不匹配消息的 ACK 都报错");

            // 另一个订阅端 ACK 不属于自己的消息
            subscribe("other", "u");
            MessageNotFoundException notMine = assertThrows(MessageNotFoundException.class,
                    () -> ack("other", m.messageId()));
            assertTrue(notMine.getMessage().contains("从未投递"));
        }

        @Test
        @DisplayName("超时未 ACK 按次数重投，超过上限进入死信且可查询，消息不消失")
        void timeoutThenDeadLetter() {
            Logger log = LoggerFactory.getLogger("场景三-重投死信");
            props.setMaxDeliveries(2);
            props.setAckTimeoutMs(-1); // 立即超时，时序完全确定
            subscribe("slow", "t");
            PublishResult m = publish("t", "x");
            DeliveryView first = pollOne("slow");
            assertFalse(first.redelivery());

            // 第 1 次超时：重投（attempt=2）
            broker.redeliverTimedOut();
            DeliveryView retry = pollOne("slow");
            assertTrue(retry.redelivery());
            assertEquals(2, retry.deliveryAttempt());
            log.info("[用例] 第 1 次超时 -> attempt=2 重投，messageId 不变 {}", m.messageId());

            // 重投副本取出时计时窗口刷新到"现在"；保持立即超时，模拟再等一个周期
            // 第 2 次超时：达到 maxDeliveries=2 -> 死信，且不再出现在 poll
            broker.redeliverTimedOut();
            assertTrue(pollAll("slow").isEmpty(), "死信后不再投递");
            List<DeadLetter> letters = broker.deadLetters("slow");
            assertEquals(1, letters.size());
            assertEquals(m.messageId(), letters.get(0).messageId());
            assertTrue(letters.get(0).reason().contains("MAX_DELIVERIES_EXCEEDED"));
            log.info("[用例] 重投达上限 -> 死信可查: reason={}", letters.get(0).reason());

            // 死信消息再 ACK 必须明确报错
            ApiException ex = assertThrows(ApiException.class, () -> ack("slow", m.messageId()));
            assertEquals("DEAD_LETTERED", ex.getErrorCode());

            // 死信接口不带 clientId 时返回全部
            assertEquals(1, broker.deadLetters(null).size());
        }

        @Test
        @DisplayName("ACK 后不再重投")
        void ackStopsRedelivery() {
            Logger log = LoggerFactory.getLogger("场景三-确认边界");
            props.setAckTimeoutMs(-1);
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            pollOne("c");
            ack("c", m.messageId());
            assertEquals(0, broker.redeliverTimedOut());
            assertTrue(pollAll("c").isEmpty());
            log.info("[用例] 已 ACK 的消息不会再被超时扫描重投");
        }
    }

    // ------------------------------------------------- 场景四：积压与订阅端隔离

    @Nested
    @DisplayName("场景四：慢订阅端积压策略与订阅端隔离")
    class Backlog {

        private BrokerProperties tightProps() {
            BrokerProperties p = new BrokerProperties();
            p.setMaxPendingPerSubscription(2);
            p.setAckTimeoutMs(1_000_000);
            return p;
        }

        @Test
        @DisplayName("dead-letter 策略（默认）：窗口满后新消息直接进死信，窗口内消息保留并仍可 ACK")
        void deadLetterStrategy() {
            Logger log = LoggerFactory.getLogger("场景四-积压-deadletter");
            BrokerService b = brokerWith(tightProps());
            b.subscribe("slow", "t");
            PublishResult m1 = publishVia(b, "t", "1");
            PublishResult m2 = publishVia(b, "t", "2");
            PublishResult m3 = publishVia(b, "t", "3");
            log.info("[用例] 上限=2，第三条发布应返回 OVERFLOW_DEADLETTER");

            assertEquals(List.of("1", "2"),
                    payloads(b.poll("slow", 10)), "窗口内仍是最旧两条");
            List<DeadLetter> letters = b.deadLetters("slow");
            assertEquals(1, letters.size());
            assertEquals(m3.messageId(), letters.get(0).messageId());
            assertTrue(letters.get(0).reason().contains("PENDING_OVERFLOW"));

            // 窗口内消息确认一条后，行为正常；被死信的消息确认报错
            b.acknowledge("slow", m1.messageId());
            assertThrows(ApiException.class, () -> b.acknowledge("slow", m3.messageId()));
            log.info("[用例结果] 窗口保留 {}、{}；溢出消息 {} 可在死信查到",
                    m1.messageId(), m2.messageId(), m3.messageId());
        }

        @Test
        @DisplayName("drop-oldest 策略：窗口满后驱逐最旧消息进死信，新消息进入窗口")
        void dropOldestStrategy() {
            Logger log = LoggerFactory.getLogger("场景四-积压-dropoldest");
            BrokerProperties p = tightProps();
            p.setOverflowStrategy("drop-oldest");
            BrokerService b = brokerWith(p);
            b.subscribe("slow", "t");
            PublishResult m1 = publishVia(b, "t", "1");
            PublishResult m2 = publishVia(b, "t", "2");
            PublishResult m3 = publishVia(b, "t", "3");

            List<DeliveryView> got = b.poll("slow", 10);
            assertEquals(List.of("2", "3"), payloads(got), "最旧的 1 被驱逐，窗口为 2,3");
            List<DeadLetter> letters = b.deadLetters("slow");
            assertEquals(m1.messageId(), letters.get(0).messageId());
            assertTrue(letters.get(0).reason().contains("drop-oldest"));
            // 新消息仍可正常确认，进度不回退
            b.acknowledge("slow", m2.messageId());
            assertThrows(ApiException.class, () -> b.acknowledge("slow", m1.messageId()));
            log.info("[用例结果] 最旧消息 {} 进死信，新消息 {} 被接收", m1.messageId(), m3.messageId());
        }

        @Test
        @DisplayName("订阅端隔离：慢订阅端积压/超时不影响快订阅端继续实时收消息")
        void isolation() {
            Logger log = LoggerFactory.getLogger("场景四-隔离");
            BrokerProperties p = tightProps();
            p.setOverflowStrategy("drop-oldest");
            BrokerService b = brokerWith(p);
            b.subscribe("slow", "t");
            b.subscribe("fast", "t");

            // 每条发布后 fast 立即消费确认，slow 从不 poll
            for (int i = 1; i <= 5; i++) {
                publishVia(b, "t", String.valueOf(i));
                List<DeliveryView> batch = b.poll("fast", 10);
                for (DeliveryView d : batch) {
                    b.acknowledge("fast", d.messageId());
                }
            }
            assertTrue(b.poll("fast", 10).isEmpty());
            assertEquals(0, b.deadLetters("fast").size(), "快客户端不应有死信");
            // slow 这边积压 5 条、上限 2：3 条最旧消息被驱逐到它自己的死信
            List<DeadLetter> slowDead = b.deadLetters("slow");
            assertEquals(3, slowDead.size(), "慢客户端自己的积压自己承担");
            log.info("[用例结果] fast 正常消费 5 条零死信；slow 独立积压，{} 条进自己的死信",
                    slowDead.size());
        }

        private PublishResult publishVia(BrokerService b, String topic, String payload) {
            return b.publish(new PublishRequest(topic, payload));
        }
    }

    private List<String> payloads(List<DeliveryView> views) {
        return views.stream().map(DeliveryView::payload).collect(Collectors.toList());
    }
}
