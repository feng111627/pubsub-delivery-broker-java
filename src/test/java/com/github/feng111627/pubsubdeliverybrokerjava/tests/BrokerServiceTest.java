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
import java.util.concurrent.CopyOnWriteArrayList;
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

    // ------------------------------------------------- 场景六：投递续期

    @Nested
    @DisplayName("场景六：投递续期（消费者自己续命）")
    class ExtendDeadline {

        @Test
        @DisplayName("续期后在续期窗口内不再被误判超时重投；续期不等于确认")
        void extendPreventsFalseTimeout() {
            Logger log = LoggerFactory.getLogger("场景六-投递续期");
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            pollOne("c"); // 客户端拿到消息，超时计时开始
            props.setAckTimeoutMs(-1); // 让"未续期"的消息立即超时，时序确定
            log.info("[用例] 已超时配置下先续期 60s，扫描不应产生重投");

            ExtendResult r = broker.extend("c", m.messageId(), 60_000L);
            assertEquals(m.globalSeq(), r.seq());
            assertTrue(r.extendedUntil() > System.currentTimeMillis());

            assertEquals(0, broker.redeliverTimedOut(), "续期窗口未到期，不得误判超时");
            // 在线客户端继续 poll 仍会拿到在途消息（既有"在线刷新计时"语义），
            // 关键断言：它不是被误判超时产生的重投副本
            List<DeliveryView> again = pollAll("c");
            assertEquals(1, again.size());
            assertFalse(again.get(0).redelivery(), "不得被当成超时重投");
            assertEquals(1, again.get(0).deliveryAttempt(), "投递次数不得因续期+拉取而增加");
            log.info("[用例结果] 续期到点={}，本轮扫描零重投，再拉取仍是 attempt=1 非重投",
                    r.extendedUntil());

            // 续期不等于确认：消息仍在途，仍需 ACK；ACK 后不再超时
            ack("c", m.messageId());
            assertEquals(0, broker.redeliverTimedOut());
            assertTrue(broker.deadLetters("c").isEmpty());
            log.info("[用例结果] 续期后正常 ACK 成功，消息完成、未进死信");
        }

        @Test
        @DisplayName("续期只延不缩：第二次更短的续期不会把到点提前")
        void extendIsMonotonic() {
            Logger log = LoggerFactory.getLogger("场景六-投递续期");
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            pollOne("c");
            ExtendResult first = broker.extend("c", m.messageId(), 60_000L);
            ExtendResult second = broker.extend("c", m.messageId(), 1L);
            assertTrue(second.extendedUntil() >= first.extendedUntil(),
                    "续期到点只能往后延，不能被更短的续期提前");
            log.info("[用例结果] 到点 {} -> {}（单调不回退）",
                    first.extendedUntil(), second.extendedUntil());
        }

        @Test
        @DisplayName("续期窗口到期后仍未 ACK：照常重投、次数累计、到上限进死信")
        void extendThenExpiryRedelivers() throws Exception {
            Logger log = LoggerFactory.getLogger("场景六-投递续期");
            props.setMaxDeliveries(2);
            props.setAckTimeoutMs(-1); // 正常窗口立即超时
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            pollOne("c");

            broker.extend("c", m.messageId(), 30L);
            assertEquals(0, broker.redeliverTimedOut(), "续期窗口未到期不重投");
            log.info("[用例] 续期 30ms 期间零重投；等待续期到期后应正常重投");

            Thread.sleep(50); // 等续期窗口到期
            broker.redeliverTimedOut();
            DeliveryView retry = pollOne("c");
            assertNotNull(retry, "续期到期后仍超时，应进入重投流程");
            assertTrue(retry.redelivery());
            assertEquals(2, retry.deliveryAttempt());
            assertEquals(m.messageId(), retry.messageId());
            log.info("[用例结果] 续期到期未 ACK -> attempt=2 重投，messageId 不变");

            // 重投后仍不确认，达到上限 -> 死信（重投次数照常累计）
            broker.redeliverTimedOut();
            List<DeadLetter> letters = broker.deadLetters("c");
            assertEquals(1, letters.size());
            assertTrue(letters.get(0).reason().contains("MAX_DELIVERIES_EXCEEDED"));
            log.info("[用例结果] 再次超时达上限 -> 死信: {}", letters.get(0).reason());
        }

        @Test
        @DisplayName("对已确认/死信/从未投递/不存在消息的续期都明确报错；非法续期时长拒绝")
        void extendErrors() {
            Logger log = LoggerFactory.getLogger("场景六-投递续期");
            props.setMaxDeliveries(1);
            props.setAckTimeoutMs(-1);
            subscribe("c", "t");
            subscribe("other", "u");
            PublishResult acked = publish("t", "acked");
            PublishResult dead = publish("t", "dead");
            pollAll("c");
            ack("c", acked.messageId());
            broker.redeliverTimedOut(); // dead 进入死信（maxDeliveries=1）

            AlreadyAckedException ex1 = assertThrows(AlreadyAckedException.class,
                    () -> broker.extend("c", acked.messageId(), 1000L));
            assertEquals("ALREADY_ACKED", ex1.getErrorCode());
            log.info("[用例] 已确认消息续期 -> ALREADY_ACKED: {}", ex1.getMessage());

            ApiException ex2 = assertThrows(ApiException.class,
                    () -> broker.extend("c", dead.messageId(), 1000L));
            assertEquals("DEAD_LETTERED", ex2.getErrorCode());
            log.info("[用例] 死信消息续期 -> DEAD_LETTERED: {}", ex2.getMessage());

            MessageNotFoundException ex3 = assertThrows(MessageNotFoundException.class,
                    () -> broker.extend("c", "msg-nope", 1000L));
            assertEquals("MESSAGE_NOT_FOUND", ex3.getErrorCode());

            MessageNotFoundException ex4 = assertThrows(MessageNotFoundException.class,
                    () -> broker.extend("other", acked.messageId(), 1000L));
            assertTrue(ex4.getMessage().contains("从未投递"));
            log.info("[用例] 不存在消息/从未投递给本订阅端的消息续期 -> MESSAGE_NOT_FOUND");

            ApiException ex5 = assertThrows(ApiException.class,
                    () -> broker.extend("c", dead.messageId(), 0L));
            assertEquals("INVALID_EXTENSION", ex5.getErrorCode());
            ApiException ex6 = assertThrows(ApiException.class,
                    () -> broker.extend("c", dead.messageId(), props.getMaxExtendMs() + 1L));
            assertEquals("INVALID_EXTENSION", ex6.getErrorCode());
            log.info("[用例] extendMs<=0 或超过 max-extend-ms -> INVALID_EXTENSION");
        }
    }

    // ------------------------------------------------- 场景七：死信再投递

    @Nested
    @DisplayName("场景七：死信再投递（捞回重投）")
    class DeadLetterRedeliver {

        /** 让已发布的消息全部超时进死信（maxDeliveries=1 + 立即超时）。 */
        private void deadLetterAll(String clientId) {
            pollAll(clientId);
            broker.redeliverTimedOut();
        }

        @Test
        @DisplayName("死信捞回后按正常流程再收到并确认；批内顺序与发布先后一致")
        void redeliverFlowAndOrder() {
            Logger log = LoggerFactory.getLogger("场景七-死信再投递");
            props.setMaxDeliveries(1);
            props.setAckTimeoutMs(-1);
            subscribe("c", "t");
            PublishResult m1 = publish("t", "1");
            PublishResult m2 = publish("t", "2");
            PublishResult m3 = publish("t", "3");
            deadLetterAll("c");
            assertEquals(3, broker.deadLetters("c").size());
            log.info("[用例] 3 条消息进死信后，按乱序请求 [{} {} {}] 批量捞回",
                    m3.messageId(), m1.messageId(), m2.messageId());

            // 故意用乱序请求：投出顺序仍必须是发布先后（seq 升序）
            RedeliverResult r = broker.redeliverDeadLetters("c",
                    List.of(m3.messageId(), m1.messageId(), m2.messageId()));
            assertEquals(3, r.redelivered());
            assertEquals(0, r.alreadyRedelivered());

            List<DeliveryView> got = pollAll("c");
            assertEquals(List.of("1", "2", "3"), payloads(got),
                    "捞回后投出顺序必须与发布先后一致");
            assertEquals(List.of(m1.globalSeq(), m2.globalSeq(), m3.globalSeq()),
                    got.stream().map(DeliveryView::seq).toList(), "seq 不变、进度不回退");
            log.info("[用例结果] 捞回 3 条，投出顺序={}（发布先后）", payloads(got));

            // 台账能看出已重新投过
            List<DeadLetter> letters = broker.deadLetters("c");
            assertTrue(letters.stream().allMatch(DeadLetter::redelivered),
                    "台账必须标记已再投递，避免反复捞");
            // 按正常流程确认
            for (DeliveryView v : got) {
                ack("c", v.messageId());
            }
            assertTrue(pollAll("c").isEmpty());
            log.info("[用例结果] 捞回消息全部 ACK 完成，台账 redeliveredAt 已标记");
        }

        @Test
        @DisplayName("同一批重复触发再投递：幂等，不乱序、不成倍重复")
        void redeliverIdempotent() {
            Logger log = LoggerFactory.getLogger("场景七-死信再投递");
            props.setMaxDeliveries(1);
            props.setAckTimeoutMs(-1);
            subscribe("c", "t");
            PublishResult m1 = publish("t", "1");
            PublishResult m2 = publish("t", "2");
            deadLetterAll("c");
            List<String> batch = List.of(m1.messageId(), m2.messageId());

            RedeliverResult first = broker.redeliverDeadLetters("c", batch);
            assertEquals(2, first.redelivered());
            RedeliverResult second = broker.redeliverDeadLetters("c", batch);
            assertEquals(0, second.redelivered());
            assertEquals(2, second.alreadyRedelivered());
            log.info("[用例] 同一批连续触发两次：第一次捞回 2，第二次全部幂等跳过");

            List<DeliveryView> got = pollAll("c");
            assertEquals(2, got.size(), "重复触发不能产生成倍重复投递");
            assertEquals(List.of("1", "2"), payloads(got));
            assertEquals(2, broker.deadLetters("c").size(), "台账记录数不膨胀");
            // 确认后在途清空，不会再有任何投出
            for (DeliveryView v : got) {
                ack("c", v.messageId());
            }
            assertTrue(pollAll("c").isEmpty());
            log.info("[用例结果] 去重后恰好收到 2 条、顺序稳定、台账不膨胀、确认后清空");
        }

        @Test
        @DisplayName("对已确认/不存在/非死信消息发起再投递，明确拒绝并说清原因")
        void redeliverErrors() {
            Logger log = LoggerFactory.getLogger("场景七-死信再投递");
            props.setMaxDeliveries(1);
            props.setAckTimeoutMs(-1);
            subscribe("c", "t");
            subscribe("other", "u");
            PublishResult acked = publish("t", "acked");
            PublishResult dead = publish("t", "dead");
            PublishResult inflight = publish("t", "inflight");
            pollAll("c");
            ack("c", acked.messageId());
            broker.redeliverTimedOut(); // dead 进死信；inflight 也会进死信，重新发布一条保持在途
            PublishResult inflight2 = publish("t", "inflight2");
            pollOne("c");

            AlreadyAckedException ex1 = assertThrows(AlreadyAckedException.class,
                    () -> broker.redeliverDeadLetters("c", List.of(acked.messageId())));
            assertEquals("ALREADY_ACKED", ex1.getErrorCode());
            log.info("[用例] 已确认消息再投递 -> ALREADY_ACKED: {}", ex1.getMessage());

            MessageNotFoundException ex2 = assertThrows(MessageNotFoundException.class,
                    () -> broker.redeliverDeadLetters("c", List.of("msg-nope")));
            assertEquals("MESSAGE_NOT_FOUND", ex2.getErrorCode());

            ApiException ex3 = assertThrows(ApiException.class,
                    () -> broker.redeliverDeadLetters("c", List.of(inflight2.messageId())));
            assertEquals("NOT_DEAD_LETTERED", ex3.getErrorCode());
            log.info("[用例] 在途消息再投递 -> NOT_DEAD_LETTERED: {}", ex3.getMessage());

            MessageNotFoundException ex4 = assertThrows(MessageNotFoundException.class,
                    () -> broker.redeliverDeadLetters("other", List.of(dead.messageId())));
            assertTrue(ex4.getMessage().contains("从未投递"));

            ApiException ex5 = assertThrows(ApiException.class,
                    () -> broker.redeliverDeadLetters("c", List.of()));
            assertEquals("INVALID_REQUEST", ex5.getErrorCode());
            log.info("[用例] 不存在/别人订阅端/空批次的再投递均明确拒绝");

            // 整批原子性：批里混一条坏消息，整批拒绝，好消息也不捞
            String cid = "atomic";
            BrokerService b2 = brokerWith(props);
            b2.subscribe(cid, "t");
            PublishResult d1 = b2.publish(new PublishRequest("t", "d1"));
            b2.poll(cid, 10);
            b2.redeliverTimedOut();
            assertThrows(MessageNotFoundException.class,
                    () -> b2.redeliverDeadLetters(cid, List.of(d1.messageId(), "msg-nope")));
            assertFalse(b2.deadLetters(cid).get(0).redelivered(),
                    "整批校验失败时任何消息都不应被捞回");
            log.info("[用例结果] 批量校验原子：混有非法 id 的批次整体拒绝");
        }

        @Test
        @DisplayName("在途满员时再投递整批拒绝（PENDING_FULL），腾出容量后可捞回，不丢消息")
        void redeliverCapacityBoundary() {
            Logger log = LoggerFactory.getLogger("场景七-死信再投递");
            BrokerProperties p = new BrokerProperties();
            p.setMaxPendingPerSubscription(2);
            p.setAckTimeoutMs(1_000_000);
            BrokerService b = brokerWith(p);
            b.subscribe("c", "t");
            PublishResult m1 = b.publish(new PublishRequest("t", "1"));
            b.publish(new PublishRequest("t", "2"));
            PublishResult m3 = b.publish(new PublishRequest("t", "3")); // 溢出进死信
            assertEquals(1, b.deadLetters("c").size());
            log.info("[用例] 在途 2/2 已满，死信 1 条；此时捞回应整批拒绝");

            ApiException full = assertThrows(ApiException.class,
                    () -> b.redeliverDeadLetters("c", List.of(m3.messageId())));
            assertEquals("PENDING_FULL", full.getErrorCode());
            assertTrue(full.getMessage().contains("上限"));
            assertFalse(b.deadLetters("c").get(0).redelivered(), "拒绝时不得改变台账状态");
            log.info("[用例结果] PENDING_FULL: {}", full.getMessage());

            b.acknowledge("c", m1.messageId()); // 腾出一个位置
            RedeliverResult r = b.redeliverDeadLetters("c", List.of(m3.messageId()));
            assertEquals(1, r.redelivered());
            assertEquals(List.of("3"), payloads(b.poll("c", 10)).subList(0, 1));
            log.info("[用例结果] 确认一条腾出容量后捞回成功，消息未丢");
        }

        @Test
        @DisplayName("并发重复触发同一批再投递 + 并发发布：恰好捞回一次、顺序稳定")
        void concurrentRedeliverAndPublish() throws Exception {
            Logger log = LoggerFactory.getLogger("场景七-死信再投递");
            props.setMaxDeliveries(1);
            props.setAckTimeoutMs(-1);
            subscribe("c", "t");
            int dead = 30;
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < dead; i++) {
                ids.add(publish("t", "d" + i).messageId());
            }
            deadLetterAll("c");
            assertEquals(dead, broker.deadLetters("c").size());
            props.setAckTimeoutMs(3_600_000); // 后续不再超时，隔离干扰

            int threads = 4;
            ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
            CountDownLatch start = new CountDownLatch(1);
            List<java.util.concurrent.Future<RedeliverResult>> futures = new ArrayList<>();
            log.info("[用例] {} 个线程并发重复触发同一批 {} 条死信再投递，同时 1 个线程发布新消息",
                    threads, dead);
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return broker.redeliverDeadLetters("c", ids);
                }));
            }
            java.util.concurrent.Future<?> pub = pool.submit(() -> {
                start.await();
                for (int i = 0; i < 5; i++) {
                    publish("t", "new" + i);
                }
                return null;
            });
            start.countDown();
            int totalRedelivered = 0;
            for (var f : futures) {
                totalRedelivered += f.get(10, TimeUnit.SECONDS).redelivered();
            }
            pub.get(10, TimeUnit.SECONDS);
            pool.shutdown();

            assertEquals(dead, totalRedelivered,
                    "并发重复触发下，每条死信恰好被捞回一次，不能成倍重复");
            List<DeliveryView> got = pollAll("c");
            assertEquals(dead + 5, got.size(), "捞回 30 条 + 新发布 5 条，一条不丢");
            List<Long> seqs = got.stream().map(DeliveryView::seq).toList();
            List<Long> sorted = new ArrayList<>(seqs);
            java.util.Collections.sort(sorted);
            assertEquals(sorted, seqs, "捞回与新消息混排后整体仍按 seq 升序，不插队、不乱序");
            assertEquals(seqs.size(), Set.copyOf(seqs).size(), "无重复投递");
            log.info("[用例结果] 并发触发恰好捞回 {} 条，总投递 {} 条，seq 严格升序无重复",
                    totalRedelivered, got.size());
        }

        @Test
        @DisplayName("续期/再投递与发布、拉取、确认并发进行：互不阻塞、状态一致")
        void concurrentExtendRedeliverWithNormalFlow() throws Exception {
            Logger log = LoggerFactory.getLogger("场景七-死信再投递");
            props.setAckTimeoutMs(3_600_000); // 不发生超时，专注并发正确性
            subscribe("fast", "t");
            subscribe("slow", "t");
            int total = 200;
            Set<String> ackedByFast = ConcurrentHashMap.newKeySet();
            List<Throwable> unexpected = new CopyOnWriteArrayList<>();

            ExecutorService pool = Executors.newFixedThreadPool(4);
            CountDownLatch start = new CountDownLatch(1);
            // 发布线程
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < total; i++) {
                        publish("t", "m" + i);
                    }
                } catch (Throwable e) { unexpected.add(e); }
            });
            // fast 消费线程：poll + 续期 + ACK
            pool.submit(() -> {
                try {
                    start.await();
                    while (ackedByFast.size() < total) {
                        for (DeliveryView v : broker.poll("fast", 10)) {
                            if (ackedByFast.contains(v.messageId())) {
                                continue;
                            }
                            broker.extend("fast", v.messageId(), 60_000L); // 处理中续命
                            broker.acknowledge("fast", v.messageId());
                            ackedByFast.add(v.messageId());
                        }
                    }
                } catch (Throwable e) { unexpected.add(e); }
            });
            // slow 消费线程：慢消费者，验证不被 fast 拖死、也不拖死 fast
            pool.submit(() -> {
                try {
                    start.await();
                    int got = 0;
                    while (got < total) {
                        List<DeliveryView> batch = broker.poll("slow", 5);
                        got += batch.size();
                        for (DeliveryView v : batch) {
                            broker.acknowledge("slow", v.messageId());
                        }
                    }
                } catch (Throwable e) { unexpected.add(e); }
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "并发流程必须能在有限时间内完成，不能互相拖死");

            assertTrue(unexpected.isEmpty(), "并发过程不应出现意外异常: " + unexpected);
            assertEquals(total, ackedByFast.size(), "fast 应确认全部消息，无重复无遗漏");
            assertEquals(0, broker.deadLetters("fast").size());
            assertEquals(0, broker.deadLetters("slow").size());
            log.info("[用例结果] 发布/续期/ACK/双订阅端并发完成：fast 确认 {} 条，双方零死信",
                    ackedByFast.size());
        }
    }

    private List<String> payloads(List<DeliveryView> views) {
        return views.stream().map(DeliveryView::payload).collect(Collectors.toList());
    }
}
