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

    // ------------------------------------------------- 场景五：投递续期

    @Nested
    @DisplayName("场景五：投递续期（heartbeat）")
    class Heartbeat {

        @Test
        @DisplayName("续期后在新的截止时刻前不再被误判超时重投")
        void heartbeatPreventsTimeoutRedelivery() {
            Logger log = LoggerFactory.getLogger("场景五-续期");
            props.setAckTimeoutMs(-1); // 已投递消息本应"立即超时"
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            pollOne("c");
            log.info("[用例] 消息 {} 已投递且已超时窗口，续期 10s 后扫描不得重投", m.messageId());

            HeartbeatResponse resp = broker.heartbeat("c", m.messageId(), 10_000L);
            assertTrue(resp.deadlineAt() > System.currentTimeMillis());
            assertEquals(0, broker.redeliverTimedOut(), "续期截止前不得被误判超时");

            // 续期不等于确认：消息仍在途，且不会被当成已完成
            DeliveryView still = pollOne("c");
            assertEquals(m.messageId(), still.messageId());
            assertFalse(still.redelivery());
            assertEquals(1, still.deliveryAttempt(), "续期不递增投递次数");
            log.info("[用例结果] 续期后扫描 0 次重投，消息仍在途 attempt=1");
        }

        @Test
        @DisplayName("续期后仍超时且未确认：按原规则重投，次数照常累计")
        void heartbeatExpiredThenRedelivered() throws Exception {
            Logger log = LoggerFactory.getLogger("场景五-续期");
            props.setAckTimeoutMs(-1);
            props.setMaxDeliveries(3);
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            pollOne("c");
            broker.heartbeat("c", m.messageId(), 30L);
            log.info("[用例] 续期 30ms 后不再续，等续期窗口过期应正常重投");
            Thread.sleep(80);

            assertEquals(1, broker.redeliverTimedOut());
            DeliveryView retry = pollOne("c");
            assertTrue(retry.redelivery());
            assertEquals(2, retry.deliveryAttempt(), "重投次数照常累计");
            assertEquals(m.messageId(), retry.messageId());
            log.info("[用例结果] 续期过期未确认 -> attempt=2 重投，messageId 不变");
        }

        @Test
        @DisplayName("续期只续一次投递：重投后的新窗口按原超时计算，到上限照常进死信")
        void heartbeatDoesNotLeakIntoNextAttempt() throws Exception {
            Logger log = LoggerFactory.getLogger("场景五-续期");
            props.setAckTimeoutMs(-1);
            props.setMaxDeliveries(1); // 首次投递超时即死信
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            pollOne("c");
            broker.heartbeat("c", m.messageId(), 30L);
            log.info("[用例] maxDeliveries=1，续期 30ms 过期后应直接进死信而非无限挂住");
            Thread.sleep(80);

            assertEquals(1, broker.redeliverTimedOut());
            List<DeadLetter> letters = broker.deadLetters("c");
            assertEquals(1, letters.size());
            assertTrue(letters.get(0).reason().contains("MAX_DELIVERIES_EXCEEDED"));
            log.info("[用例结果] 续期不能阻止到上限进死信: reason={}", letters.get(0).reason());
        }

        @Test
        @DisplayName("续期不是确认：续期后 ACK 才完成；ACK 后再续期报 ALREADY_ACKED")
        void heartbeatIsNotAck() {
            Logger log = LoggerFactory.getLogger("场景五-续期");
            props.setAckTimeoutMs(-1);
            subscribe("c", "t");
            PublishResult m = publish("t", "x");
            pollOne("c");
            broker.heartbeat("c", m.messageId(), 10_000L);
            log.info("[用例] 续期后消息仍未完成，必须显式 ACK");

            ack("c", m.messageId());
            assertEquals(0, broker.redeliverTimedOut(), "ACK 后不再参与任何超时");
            AlreadyAckedException ex = assertThrows(AlreadyAckedException.class,
                    () -> broker.heartbeat("c", m.messageId(), 1000L));
            assertEquals("ALREADY_ACKED", ex.getErrorCode());
            log.info("[用例结果] 续期未确认消息；ACK 完成后再续期明确报 ALREADY_ACKED");
        }

        @Test
        @DisplayName("续期边界：不存在/从未投递/未拉取/已死信/超长续期全部明确报错")
        void heartbeatErrors() {
            Logger log = LoggerFactory.getLogger("场景五-续期");
            props.setAckTimeoutMs(-1);
            props.setMaxDeliveries(1);
            subscribe("c", "t");
            subscribe("other", "u");
            PublishResult m = publish("t", "x");
            log.info("[用例] 覆盖 5 类非法续期");

            MessageNotFoundException ghost = assertThrows(MessageNotFoundException.class,
                    () -> broker.heartbeat("c", "msg-nope", 1000L));
            assertEquals("MESSAGE_NOT_FOUND", ghost.getErrorCode());

            // 从未投递给该订阅端（主题不匹配）
            MessageNotFoundException notMine = assertThrows(MessageNotFoundException.class,
                    () -> broker.heartbeat("other", m.messageId(), 1000L));
            assertTrue(notMine.getMessage().contains("从未投递"));

            // 在途但尚未被 poll 取走：不存在需要续期的投递
            ApiException notYet = assertThrows(ApiException.class,
                    () -> broker.heartbeat("c", m.messageId(), 1000L));
            assertEquals("NOT_YET_DELIVERED", notYet.getErrorCode());

            // 超长续期
            pollOne("c");
            ApiException tooLong = assertThrows(ApiException.class,
                    () -> broker.heartbeat("c", m.messageId(), 999_999_999L));
            assertEquals("INVALID_HEARTBEAT", tooLong.getErrorCode());
            assertTrue(tooLong.getMessage().contains("heartbeat-max-extension-ms"));

            // 已死信
            broker.redeliverTimedOut();
            ApiException dead = assertThrows(ApiException.class,
                    () -> broker.heartbeat("c", m.messageId(), 1000L));
            assertEquals("DEAD_LETTERED", dead.getErrorCode());
            assertTrue(dead.getMessage().contains("redrive"), "错误信息应指引去再投递");
            log.info("[用例结果] MESSAGE_NOT_FOUND/从未投递/NOT_YET_DELIVERED/INVALID_HEARTBEAT/DEAD_LETTERED 均明确报错");
        }

        @Test
        @DisplayName("并发续期与超时扫描、发布并行：不错乱、不丢、其他订阅端不受影响")
        void concurrentHeartbeat() throws Exception {
            Logger log = LoggerFactory.getLogger("场景五-续期");
            props.setAckTimeoutMs(60);
            props.setHeartbeatMaxExtensionMs(10_000);
            subscribe("hb", "t");
            subscribe("bystander", "t");
            PublishResult m = publish("t", "x");
            pollOne("hb");
            log.info("[用例] 8 线程并发续期同一消息 + 1 线程持续超时扫描 + 1 线程持续发布");

            int threads = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threads + 2);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            List<Throwable> failures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int k = 0; k < 50; k++) {
                            broker.heartbeat("hb", m.messageId(), 500L);
                        }
                    } catch (Throwable t) {
                        synchronized (failures) { failures.add(t); }
                    } finally {
                        done.countDown();
                    }
                });
            }
            // 扫描线程：续期窗口 500ms 远大于扫描周期，期间不得把该消息重投
            pool.submit(() -> {
                try {
                    start.await();
                    while (done.getCount() > 0) {
                        broker.redeliverTimedOut();
                        Thread.sleep(5);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            // 发布线程：验证续期/扫描不阻塞正常发布与其他订阅端
            pool.submit(() -> {
                try {
                    start.await();
                    for (int k = 0; k < 50; k++) {
                        publish("t", "bg-" + k);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            start.countDown();
            assertTrue(done.await(15, TimeUnit.SECONDS));
            pool.shutdown();
            assertTrue(failures.isEmpty(), "并发续期不应失败: " + failures);

            DeliveryView d = pollOne("hb");
            assertEquals(m.messageId(), d.messageId());
            assertEquals(1, d.deliveryAttempt(), "持续续期期间不得被超时重投");
            ack("hb", m.messageId());
            // 旁观订阅端应能收到全部 51 条（首条 + 后台 50 条），不受 hb 续期影响
            List<DeliveryView> other = pollAll("bystander");
            assertEquals(51, other.size(), "其他订阅端投递不应被续期拖住");
            log.info("[用例结果] 并发续期 400 次零失败，消息未被误判重投，旁观订阅端收齐 51 条");
        }
    }

    // ------------------------------------------------- 场景六：死信再投递

    @Nested
    @DisplayName("场景六：死信再投递（redrive）")
    class Redrive {

        /** 让一条消息立即进死信：maxDeliveries=1 + 立即超时。 */
        private BrokerService deadLetterBroker() {
            BrokerProperties p = new BrokerProperties();
            p.setAckTimeoutMs(-1);
            p.setMaxDeliveries(1);
            return brokerWith(p);
        }

        @Test
        @DisplayName("死信再投递后按正常流程收到并确认；台账可见已再投状态")
        void redriveBasicFlow() {
            Logger log = LoggerFactory.getLogger("场景六-再投递");
            BrokerService b = deadLetterBroker();
            b.subscribe("c", "t");
            PublishResult m = b.publish(new PublishRequest("t", "x"));
            b.poll("c", 10);
            b.redeliverTimedOut();
            assertEquals(1, b.deadLetters("c").size());
            log.info("[用例] 消息 {} 已进死信，发起再投递", m.messageId());

            RedriveResponse resp = b.redrive("c", List.of(m.messageId()));
            assertEquals(1, resp.count());
            assertEquals(List.of(m.messageId()), resp.messageIds());

            List<DeliveryView> got = b.poll("c", 10);
            assertEquals(1, got.size());
            assertEquals(m.messageId(), got.get(0).messageId());
            assertEquals(1, got.get(0).deliveryAttempt(), "再投递按全新一次投递计时");
            assertFalse(got.get(0).redelivery());

            // 台账能看出已再投
            List<DeadLetterView> views = b.deadLetterViews("c");
            assertTrue(views.get(0).redriven());
            assertNotNull(views.get(0).redrivenAt());

            // 正常 ACK 完成，之后不再出现
            b.acknowledge("c", m.messageId());
            assertTrue(b.poll("c", 10).isEmpty());
            log.info("[用例结果] 再投递->收到->ACK 完成；台账 redriven=true");
        }

        @Test
        @DisplayName("同一批重复再投递：第二次明确拒绝，不会成倍重复")
        void duplicateRedriveRejected() {
            Logger log = LoggerFactory.getLogger("场景六-再投递");
            BrokerService b = deadLetterBroker();
            b.subscribe("c", "t");
            PublishResult m = b.publish(new PublishRequest("t", "x"));
            b.poll("c", 10);
            b.redeliverTimedOut();
            b.redrive("c", List.of(m.messageId()));
            log.info("[用例] 同一消息再次 redrive 必须明确拒绝且不出第二份拷贝");

            ApiException dup = assertThrows(ApiException.class,
                    () -> b.redrive("c", List.of(m.messageId())));
            assertEquals("NOT_DEAD_LETTERED", dup.getErrorCode());
            assertTrue(dup.getMessage().contains(m.messageId()));

            List<DeliveryView> got = b.poll("c", 10);
            assertEquals(1, got.size(), "重复再投递不得产生第二份拷贝");
            log.info("[用例结果] 第二次 redrive 报 NOT_DEAD_LETTERED，poll 仅 1 份");
        }

        @Test
        @DisplayName("批量再投递保持原发布顺序；乱序入参、重复触发都不乱序不重复")
        void redriveBatchOrdering() {
            Logger log = LoggerFactory.getLogger("场景六-再投递");
            BrokerService b = deadLetterBroker();
            b.subscribe("c", "t");
            PublishResult m1 = b.publish(new PublishRequest("t", "1"));
            PublishResult m2 = b.publish(new PublishRequest("t", "2"));
            PublishResult m3 = b.publish(new PublishRequest("t", "3"));
            b.poll("c", 10);
            b.redeliverTimedOut();
            assertEquals(3, b.deadLetters("c").size());
            log.info("[用例] 3 条死信，按乱序 [m3,m1,m2] 提交再投递");

            RedriveResponse resp = b.redrive("c",
                    List.of(m3.messageId(), m1.messageId(), m2.messageId()));
            assertEquals(List.of(m1.messageId(), m2.messageId(), m3.messageId()),
                    resp.messageIds(), "响应必须按原发布顺序（全局序号升序）");
            assertEquals(List.of(m1.globalSeq(), m2.globalSeq(), m3.globalSeq()), resp.seqs());

            List<DeliveryView> got = b.poll("c", 10);
            assertEquals(List.of("1", "2", "3"), payloads(got),
                    "批内相对先后必须与原发布先后一致");
            log.info("[用例结果] 乱序入参仍按发布顺序 {} 投递", resp.seqs());
        }

        @Test
        @DisplayName("再投递落回正确相对位置：插到在途窗口中自己原来的序号位置，不插队")
        void redriveLandsAtOriginalPosition() {
            Logger log = LoggerFactory.getLogger("场景六-再投递");
            BrokerService b = deadLetterBroker();
            b.subscribe("c", "t");
            PublishResult m1 = b.publish(new PublishRequest("t", "1"));
            PublishResult m2 = b.publish(new PublishRequest("t", "2"));
            b.poll("c", 10);
            b.redeliverTimedOut(); // m1,m2 进死信
            PublishResult m3 = b.publish(new PublishRequest("t", "3")); // 仍在途、未拉取
            log.info("[用例] m1,m2 死信后又有新消息 m3 在途，再投递 m1,m2 应落在 m3 之前（原序号位置）");

            b.redrive("c", List.of(m2.messageId(), m1.messageId()));
            List<DeliveryView> got = b.poll("c", 10);
            assertEquals(List.of("1", "2", "3"), payloads(got),
                    "再投递消息按原序号回到窗口，不打乱在途顺序");
            assertEquals(m3.messageId(), got.get(2).messageId());
            log.info("[用例结果] poll 顺序 1,2,3：再投递消息未插队");
        }

        @Test
        @DisplayName("再投递边界：已确认/不存在/从未投递明确报错；整批原子拒绝")
        void redriveErrors() {
            Logger log = LoggerFactory.getLogger("场景六-再投递");
            BrokerService b = deadLetterBroker();
            b.subscribe("c", "t");
            subscribe(b, "other", "u");
            PublishResult acked = b.publish(new PublishRequest("t", "acked"));
            PublishResult dead = b.publish(new PublishRequest("t", "dead"));
            b.poll("c", 10);
            b.acknowledge("c", acked.messageId());
            b.redeliverTimedOut(); // 仅 dead 进死信（acked 已完成）
            log.info("[用例] 覆盖已确认/不存在/从未投递/整批原子性");

            AlreadyAckedException ackedEx = assertThrows(AlreadyAckedException.class,
                    () -> b.redrive("c", List.of(acked.messageId())));
            assertEquals("ALREADY_ACKED", ackedEx.getErrorCode());

            MessageNotFoundException ghost = assertThrows(MessageNotFoundException.class,
                    () -> b.redrive("c", List.of("msg-nope")));
            assertEquals("MESSAGE_NOT_FOUND", ghost.getErrorCode());

            MessageNotFoundException notMine = assertThrows(MessageNotFoundException.class,
                    () -> b.redrive("other", List.of(dead.messageId())));
            assertTrue(notMine.getMessage().contains("从未投递"));

            // 整批原子：好 id + 坏 id 一起提交 -> 整批拒绝，好 id 仍在死信中
            assertThrows(MessageNotFoundException.class,
                    () -> b.redrive("c", List.of(dead.messageId(), "msg-nope")));
            assertFalse(b.deadLetterViews("c").get(0).redriven(),
                    "整批拒绝后任何一条都不得被再投");
            log.info("[用例结果] 三类错误明确报错；含坏 id 的批次整体不生效");
        }

        @Test
        @DisplayName("容量边界：在途窗口放不下整批时明确拒绝，不截断不丢消息；超限批量拒绝")
        void redriveCapacity() {
            Logger log = LoggerFactory.getLogger("场景六-再投递");
            BrokerProperties p = new BrokerProperties();
            p.setAckTimeoutMs(-1);
            p.setMaxDeliveries(1);
            p.setMaxPendingPerSubscription(2);
            p.setMaxRedriveBatch(2);
            BrokerService b = brokerWith(p);
            b.subscribe("c", "t");
            PublishResult d1 = b.publish(new PublishRequest("t", "d1"));
            PublishResult d2 = b.publish(new PublishRequest("t", "d2"));
            b.poll("c", 10);
            b.redeliverTimedOut(); // d1,d2 死信，窗口腾空
            b.publish(new PublishRequest("t", "n1"));
            b.publish(new PublishRequest("t", "n2")); // 窗口重新占满 2/2
            log.info("[用例] 窗口满(2/2)时再投递 2 条死信，必须明确拒绝");

            ApiException full = assertThrows(ApiException.class,
                    () -> b.redrive("c", List.of(d1.messageId(), d2.messageId())));
            assertEquals("REDRIVE_CAPACITY_EXCEEDED", full.getErrorCode());
            assertTrue(full.getMessage().contains("剩余容量 0"));
            assertFalse(b.deadLetterViews("c").get(0).redriven(), "容量拒绝后不得部分生效");

            // 超限批量
            ApiException tooBig = assertThrows(ApiException.class,
                    () -> b.redrive("c", List.of(d1.messageId(), d2.messageId(), "x")));
            assertEquals("REDRIVE_BATCH_TOO_LARGE", tooBig.getErrorCode());

            // 确认一条腾出容量后，单条再投递成功
            List<DeliveryView> inflight = b.poll("c", 10);
            b.acknowledge("c", inflight.get(0).messageId());
            RedriveResponse ok = b.redrive("c", List.of(d1.messageId()));
            assertEquals(1, ok.count());
            log.info("[用例结果] 容量满拒绝且不部分生效；腾出容量后单条再投递成功");
        }

        @Test
        @DisplayName("并发再投递同一批：只有一个成功，消息不成倍、顺序不乱；与发布并发不互相拖死")
        void concurrentRedrive() throws Exception {
            Logger log = LoggerFactory.getLogger("场景六-再投递");
            BrokerService b = deadLetterBroker();
            b.subscribe("c", "t");
            List<String> ids = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                ids.add(b.publish(new PublishRequest("t", "m" + i)).messageId());
            }
            b.poll("c", 100);
            b.redeliverTimedOut();
            assertEquals(10, b.deadLetters("c").size());
            log.info("[用例] 4 线程并发 redrive 同一批 10 条 + 2 线程并发发布");

            int threads = 4;
            ExecutorService pool = Executors.newFixedThreadPool(threads + 2);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads + 2);
            List<RedriveResponse> successes = new ArrayList<>();
            List<Throwable> rejections = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        RedriveResponse r = b.redrive("c", ids);
                        synchronized (successes) { successes.add(r); }
                    } catch (Throwable t) {
                        synchronized (rejections) { rejections.add(t); }
                    } finally {
                        done.countDown();
                    }
                });
            }
            for (int i = 0; i < 2; i++) {
                int n = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int k = 0; k < 20; k++) {
                            b.publish(new PublishRequest("t", "bg" + n + "-" + k));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(15, TimeUnit.SECONDS));
            pool.shutdown();

            assertEquals(1, successes.size(), "同一批并发再投递只能成功一次");
            assertEquals(threads - 1, rejections.size());
            for (Throwable t : rejections) {
                assertEquals("NOT_DEAD_LETTERED", ((ApiException) t).getErrorCode());
            }
            // 再投递的 10 条 + 后台 40 条：poll 严格按序号升序、无重复
            List<DeliveryView> got = b.poll("c", 1000);
            assertEquals(50, got.size(), "再投递 10 条 + 并发发布 40 条，不得重复不得丢");
            List<Long> seqs = got.stream().map(DeliveryView::seq).collect(Collectors.toList());
            List<Long> sorted = new ArrayList<>(seqs);
            java.util.Collections.sort(sorted);
            assertEquals(sorted, seqs, "并发下投递顺序必须稳定");
            assertEquals(seqs.size(), Set.copyOf(seqs).size(), "不得成倍重复");
            log.info("[用例结果] 并发 redrive 仅 1 次成功，共 {} 条按序投递、零重复", got.size());
        }

        private void subscribe(BrokerService b, String clientId, String filter) {
            b.subscribe(clientId, filter);
        }
    }

    private List<String> payloads(List<DeliveryView> views) {
        return views.stream().map(DeliveryView::payload).collect(Collectors.toList());
    }
}
