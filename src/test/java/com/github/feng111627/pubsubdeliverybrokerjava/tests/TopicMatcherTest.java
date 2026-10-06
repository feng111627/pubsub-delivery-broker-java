package com.github.feng111627.pubsubdeliverybrokerjava.tests;

import com.github.feng111627.pubsubdeliverybrokerjava.exception.InvalidSubscriptionException;
import com.github.feng111627.pubsubdeliverybrokerjava.topic.TopicMatcher;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：层级主题过滤与通配匹配规则。
 */
class TopicMatcherTest {

    private static final Logger log = LoggerFactory.getLogger("场景-主题匹配");

    @Test
    void 精确匹配与不匹配() {
        log.info("[用例] 精确主题：相等匹配、不同主题不匹配");
        assertTrue(TopicMatcher.matches("orders/new", "orders/new"));
        assertFalse(TopicMatcher.matches("orders/new", "orders/old"));
        assertFalse(TopicMatcher.matches("orders/new", "orders/new/york"));
    }

    @Test
    void 单层通配符加号() {
        log.info("[用例] '+' 恰好匹配一层");
        assertTrue(TopicMatcher.matches("orders/+/shipped", "orders/new/shipped"));
        assertFalse(TopicMatcher.matches("orders/+/shipped", "orders/new/york/shipped"));
        assertFalse(TopicMatcher.matches("orders/+", "orders"));
        assertTrue(TopicMatcher.matches("+", "anything"));
    }

    @Test
    void 多层通配符井号() {
        log.info("[用例] '#' 匹配零层或任意多层后缀");
        assertTrue(TopicMatcher.matches("orders/#", "orders"));
        assertTrue(TopicMatcher.matches("orders/#", "orders/new/york/shipped"));
        assertFalse(TopicMatcher.matches("orders/#", "payment/new"));
        assertTrue(TopicMatcher.matches("#", "a/b/c"));
    }

    @Test
    void 非法filter必须明确拒绝并说明位置() {
        log.info("[用例] 非法 filter：空、空层、通配符不独占、#不在末尾");
        assertRejected(null, "不能为空");
        assertRejected("  ", "不能为空");
        assertRejected("orders//new", "空层级");
        assertRejected("/orders", "空层级");
        assertRejected("orders/", "空层级");
        assertRejected("orders/ab+", "独占");
        assertRejected("orders/a#b", "独占");
        assertRejected("orders/#/tail", "最后一层");
        assertRejected("orders/a b", "空白");
    }

    @Test
    void 发布主题不允许通配符() {
        log.info("[用例] 发布主题含 +/# 或空层必须拒绝");
        assertDoesNotThrow(() -> TopicMatcher.validatePublishTopic("orders/new/york"));
        InvalidSubscriptionException e = assertThrows(InvalidSubscriptionException.class,
                () -> TopicMatcher.validatePublishTopic("orders/+"));
        assertTrue(e.getMessage().contains("通配符"), e.getMessage());
        assertThrows(InvalidSubscriptionException.class,
                () -> TopicMatcher.validatePublishTopic("orders/#"));
        assertThrows(InvalidSubscriptionException.class,
                () -> TopicMatcher.validatePublishTopic("a//b"));
    }

    private static void assertRejected(String filter, String reasonPart) {
        InvalidSubscriptionException e = assertThrows(InvalidSubscriptionException.class,
                () -> TopicMatcher.validateFilter(filter),
                "filter 应被拒绝: " + filter);
        assertTrue(e.getMessage().contains(reasonPart),
                "错误信息应包含 '" + reasonPart + "'，实际: " + e.getMessage());
        assertEquals("INVALID_SUBSCRIPTION", e.getErrorCode());
    }
}
