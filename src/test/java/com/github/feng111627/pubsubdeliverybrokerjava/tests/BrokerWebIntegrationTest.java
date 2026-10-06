package com.github.feng111627.pubsubdeliverybrokerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 场景五：HTTP 端到端。覆盖完整 REST 生命周期、错误响应格式，
 * 以及后台调度器在小超时配置下"真实地"把消息重投/送入死信。
 */
@SpringBootTest(properties = {
        "broker.ack-timeout-ms=100",
        "broker.max-deliveries=2",
        "broker.max-pending-per-subscription=2",
        "broker.overflow-strategy=dead-letter",
        "broker.redelivery-scan-ms=20"
})
@AutoConfigureMockMvc
class BrokerWebIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger("场景五-HTTP端到端");

    @Autowired
    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void resetState() {
        // 上下文跨用例共享，这里通过使用不同 clientId/topic 前缀来隔离状态
    }

    @Test
    @DisplayName("完整生命周期：订阅->发布->轮询->ACK；非法订阅/重复ACK/死信查询")
    void endToEnd() throws Exception {
        String cid = "web-e2e";
        log.info("[用例] 非法订阅 a//b 期望 400 INVALID_SUBSCRIPTION");
        mvc.perform(post("/api/subscribe")
                        .contentType("application/json")
                        .content("""
                                {"clientId":"%s","filter":"a//b"}""".formatted(cid)))
                .andExpect(status().isBadRequest())
                .andExpect(r -> {
                    JsonNode body = json.readTree(r.getResponse().getContentAsString());
                    assertEquals("INVALID_SUBSCRIPTION", body.get("errorCode").asText());
                    assertTrue(body.get("message").asText().contains("空层级"));
                });

        log.info("[用例] 合法订阅 orders/#");
        mvc.perform(post("/api/subscribe")
                        .contentType("application/json")
                        .content("""
                                {"clientId":"%s","filter":"orders/#"}""".formatted(cid)))
                .andExpect(status().isOk());

        log.info("[用例] 发布 orders/new 与其他主题各一条");
        String idOrders = publishAndGetId("orders/new", "hello");
        publishAndGetId("other/topic", "noisy");

        MvcResult pollResult = mvc.perform(post("/api/subscriptions/{cid}/poll?max=10", cid))
                .andExpect(status().isOk()).andReturn();
        JsonNode batch = json.readTree(pollResult.getResponse().getContentAsString());
        assertEquals(1, batch.size(), "只应收到匹配 orders/# 的一条");
        assertEquals(idOrders, batch.get(0).get("messageId").asText());
        assertFalse(batch.get(0).get("redelivery").asBoolean());
        log.info("[用例结果] 首次投递收到 {}，未匹配主题被过滤", idOrders);

        // 模拟客户端断线：等待期间不 poll（持续 poll 会代表客户端在线并刷新超时窗口）
        log.info("[用例] 断线等待一个超时周期后一次性拉取，应得到 attempt=2 的重投副本");
        Thread.sleep(180);
        JsonNode retry = json.readTree(mvc.perform(post("/api/subscriptions/{cid}/poll?max=10", cid))
                .andReturn().getResponse().getContentAsString());
        assertEquals(idOrders, retry.get(0).get("messageId").asText());
        assertTrue(retry.get(0).get("redelivery").asBoolean());
        assertEquals(2, retry.get(0).get("deliveryAttempt").asInt());

        log.info("[用例] 重投副本也不 ACK，再等待，投递达上限后进入死信");
        Thread.sleep(200);
        JsonNode dead = json.readTree(mvc.perform(get("/api/dead-letters").param("clientId", cid))
                .andReturn().getResponse().getContentAsString());
        assertTrue(dead.size() >= 1);
        assertEquals(idOrders, dead.get(0).get("messageId").asText());
        assertTrue(dead.get(0).get("reason").asText().contains("MAX_DELIVERIES_EXCEEDED"));

        log.info("[用例] 对已死信消息 ACK 期望 400 DEAD_LETTERED");
        mvc.perform(post("/api/subscriptions/{cid}/ack", cid)
                        .contentType("application/json")
                        .content("""
                                {"messageId":"%s"}""".formatted(idOrders)))
                .andExpect(status().isBadRequest())
                .andExpect(r -> assertEquals("DEAD_LETTERED",
                        json.readTree(r.getResponse().getContentAsString()).get("errorCode").asText()));

        log.info("[用例] 对不存在消息 ACK 期望 400 MESSAGE_NOT_FOUND");
        mvc.perform(post("/api/subscriptions/{cid}/ack", cid)
                        .contentType("application/json")
                        .content("""
                                {"messageId":"msg-nope"}"""))
                .andExpect(status().isBadRequest())
                .andExpect(r -> assertEquals("MESSAGE_NOT_FOUND",
                        json.readTree(r.getResponse().getContentAsString()).get("errorCode").asText()));
    }

    @Test
    @DisplayName("重复确认已 ACK 的消息返回 400 ALREADY_ACKED，不被当成成功")
    void duplicateAckOverHttp() throws Exception {
        String cid = "web-dupack";
        log.info("[用例] 发布-投递-ACK 成功，再次 ACK 期望 400 ALREADY_ACKED");
        mvc.perform(post("/api/subscribe").contentType("application/json")
                .content("""
                        {"clientId":"%s","filter":"t"}""".formatted(cid))).andExpect(status().isOk());
        String id = publishAndGetId("t", "v");
        mvc.perform(post("/api/subscriptions/{cid}/poll", cid)).andExpect(status().isOk());
        mvc.perform(post("/api/subscriptions/{cid}/ack", cid).contentType("application/json")
                .content("""
                        {"messageId":"%s"}""".formatted(id))).andExpect(status().isNoContent());
        mvc.perform(post("/api/subscriptions/{cid}/ack", cid).contentType("application/json")
                        .content("""
                                {"messageId":"%s"}""".formatted(id)))
                .andExpect(status().isBadRequest())
                .andExpect(r -> assertEquals("ALREADY_ACKED",
                        json.readTree(r.getResponse().getContentAsString()).get("errorCode").asText()));
    }

    @Test
    @DisplayName("积压超限时发布响应可解释：OVERFLOW_DEADLETTER 且死信接口可查")
    void overflowReported() throws Exception {
        String cid = "web-overflow";
        log.info("[用例] 上限=2：连第三条发布应返回 OVERFLOW_DEADLETTER");
        mvc.perform(post("/api/subscribe").contentType("application/json")
                .content("""
                        {"clientId":"%s","filter":"q"}""".formatted(cid))).andExpect(status().isOk());
        publishAndGetId("q", "1");
        publishAndGetId("q", "2");
        MvcResult r3 = mvc.perform(post("/api/publish").contentType("application/json")
                .content("""
                        {"topic":"q","payload":"3"}""")).andReturn();
        JsonNode body = json.readTree(r3.getResponse().getContentAsString());
        assertEquals("OVERFLOW_DEADLETTER", body.get("status").asText());
        assertTrue(body.get("note").asText().contains("积压"), body.get("note").asText());
        JsonNode dead = json.readTree(mvc.perform(get("/api/dead-letters").param("clientId", cid))
                .andReturn().getResponse().getContentAsString());
        assertEquals(1, dead.size());
        assertEquals(body.get("messageId").asText(), dead.get(0).get("messageId").asText());
        assertEquals("q", dead.get(0).get("topic").asText());
        assertTrue(dead.get(0).get("reason").asText().contains("PENDING_OVERFLOW"));
        log.info("[用例结果] 发布响应 status={}，死信台账含 1 条记录", body.get("status").asText());
    }

    @Test
    @DisplayName("健康错误：未知 clientId、畸形 JSON、clientId 冲突均给出明确错误码")
    void miscErrors() throws Exception {
        log.info("[用例] 未知 clientId 轮询 -> 400 MESSAGE_NOT_FOUND");
        mvc.perform(post("/api/subscriptions/nobody/poll"))
                .andExpect(status().isBadRequest());

        mvc.perform(post("/api/subscribe").contentType("application/json")
                .content("""
                        {"clientId":"dup","filter":"a/+"}""")).andExpect(status().isOk());
        log.info("[用例] 同 clientId 不同 filter -> 409 SUBSCRIPTION_CONFLICT");
        mvc.perform(post("/api/subscribe").contentType("application/json")
                        .content("""
                                {"clientId":"dup","filter":"b/+"}"""))
                .andExpect(status().isConflict());

        log.info("[用例] 畸形 JSON -> 400 MALFORMED_REQUEST");
        mvc.perform(post("/api/publish").contentType("application/json")
                        .content("{not-json"))
                .andExpect(status().isBadRequest())
                .andExpect(r -> assertEquals("MALFORMED_REQUEST",
                        json.readTree(r.getResponse().getContentAsString()).get("errorCode").asText()));
    }

    private String publishAndGetId(String topic, String payload) throws Exception {
        MvcResult r = mvc.perform(post("/api/publish").contentType("application/json")
                .content(json.writeValueAsString(new java.util.LinkedHashMap<>() {{
                    put("topic", topic);
                    put("payload", payload);
                }}))).andExpect(status().isOk()).andReturn();
        return json.readTree(r.getResponse().getContentAsString()).get("messageId").asText();
    }
}
