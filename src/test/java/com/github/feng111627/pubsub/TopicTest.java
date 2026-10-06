package com.github.feng111627.pubsub;

import com.github.feng111627.pubsub.broker.BrokerException;
import com.github.feng111627.pubsub.broker.Topic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@DisplayName("主题匹配与校验")
class TopicTest {

    @Test
    @DisplayName("精确匹配：相同层级才匹配")
    void exactMatch() {
        Topic filter = Topic.parseFilter("orders/created");
        assertThat(filter.matches(Topic.parsePublishTopic("orders/created"))).isTrue();
        assertThat(filter.matches(Topic.parsePublishTopic("orders/cancelled"))).isFalse();
        assertThat(filter.matches(Topic.parsePublishTopic("orders/created/v2"))).isFalse();
        assertThat(filter.matches(Topic.parsePublishTopic("orders"))).isFalse();
    }

    @Test
    @DisplayName("单段通配 '+'：恰好匹配一段")
    void singleLevelWildcard() {
        Topic filter = Topic.parseFilter("orders/+/paid");
        assertThat(filter.matches(Topic.parsePublishTopic("orders/eu/paid"))).isTrue();
        assertThat(filter.matches(Topic.parsePublishTopic("orders/eu/west/paid"))).isFalse();
        assertThat(filter.matches(Topic.parsePublishTopic("orders/paid"))).isFalse();
    }

    @Test
    @DisplayName("多段通配 '#'：匹配剩余任意段（含零段）")
    void multiLevelWildcard() {
        Topic filter = Topic.parseFilter("orders/#");
        assertThat(filter.matches(Topic.parsePublishTopic("orders"))).isTrue();
        assertThat(filter.matches(Topic.parsePublishTopic("orders/eu"))).isTrue();
        assertThat(filter.matches(Topic.parsePublishTopic("orders/eu/paid"))).isTrue();
        assertThat(filter.matches(Topic.parsePublishTopic("payments/eu"))).isFalse();
    }

    @Test
    @DisplayName("'#' 单独作为过滤器匹配所有主题")
    void hashAloneMatchesEverything() {
        Topic filter = Topic.parseFilter("#");
        assertThat(filter.matches(Topic.parsePublishTopic("a"))).isTrue();
        assertThat(filter.matches(Topic.parsePublishTopic("a/b/c"))).isTrue();
    }

    @Test
    @DisplayName("非法过滤器被拒绝并说明原因：'#' 不在末段")
    void hashNotAtEndRejected() {
        assertThatThrownBy(() -> Topic.parseFilter("orders/#/paid"))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("'#' 只能出现在最后一段");
    }

    @Test
    @DisplayName("非法过滤器被拒绝并说明原因：通配符未独占一段")
    void wildcardMustOwnSegment() {
        assertThatThrownBy(() -> Topic.parseFilter("orders/pa+"))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("'+' 必须独占一段");
        assertThatThrownBy(() -> Topic.parseFilter("or#ders"))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("'#' 必须独占一段");
    }

    @Test
    @DisplayName("非法主题被拒绝并说明原因：空段与空主题")
    void emptySegmentRejected() {
        assertThatThrownBy(() -> Topic.parseFilter("orders//paid"))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("主题段不能为空");
        assertThatThrownBy(() -> Topic.parseFilter(""))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("主题不能为空");
    }

    @Test
    @DisplayName("发布主题不允许通配符")
    void publishTopicRejectsWildcard() {
        assertThatThrownBy(() -> Topic.parsePublishTopic("orders/+"))
                .isInstanceOf(BrokerException.class)
                .hasMessageContaining("发布主题不允许包含通配符");
    }
}
