package com.github.feng111627.pubsubdeliverybrokerjava.topic;

import com.github.feng111627.pubsubdeliverybrokerjava.exception.InvalidSubscriptionException;

/**
 * 层级主题：校验与 MQTT 风格通配匹配。
 *
 * 规则：
 * <ul>
 *   <li>以 '/' 分层，例如 {@code orders/new/york}；每层非空，主题中不允许出现空白字符。</li>
 *   <li>订阅 filter 支持两种通配符：
 *     <ul>
 *       <li>{@code +}：独占一层，恰好匹配任意一层，例如 {@code orders/+/shipped}。</li>
 *       <li>{@code #}：只能独占最后一层，匹配零层或任意多层后缀，例如 {@code orders/#}。</li>
 *     </ul>
 *   </li>
 *   <li>发布主题是具体主题，不允许出现 {@code +}、{@code #}，也不允许前导/结尾/重复的 '/'。</li>
 * </ul>
 */
public final class TopicMatcher {

    private static final int MAX_LENGTH = 256;

    private TopicMatcher() { }

    /**
     * 校验订阅 filter；非法时抛出 {@link InvalidSubscriptionException}，消息中说明具体位置与原因。
     */
    public static void validateFilter(String filter) {
        if (filter == null || filter.isBlank()) {
            throw new InvalidSubscriptionException("订阅 filter 不能为空");
        }
        if (filter.length() > MAX_LENGTH) {
            throw new InvalidSubscriptionException(
                    "订阅 filter 长度超过 " + MAX_LENGTH + " 字符: " + filter.length());
        }
        String[] segments = filter.split("/", -1);
        for (int i = 0; i < segments.length; i++) {
            String seg = segments[i];
            if (seg.isEmpty()) {
                throw new InvalidSubscriptionException(
                        "订阅 filter 存在空层级（不允许前导/结尾/重复的 '/'），位置: 第 " + (i + 1)
                                + " 层, filter=" + filter);
            }
            if (containsWhitespace(seg)) {
                throw new InvalidSubscriptionException(
                        "订阅 filter 的层级中不允许出现空白字符，位置: 第 " + (i + 1) + " 层, filter=" + filter);
            }
            if (seg.length() > 1 && (seg.contains("+") || seg.contains("#"))) {
                throw new InvalidSubscriptionException(
                        "通配符 '+' 与 '#' 必须独占一个层级，位置: 第 " + (i + 1)
                                + " 层, 内容=" + seg);
            }
            if ("#".equals(seg) && i != segments.length - 1) {
                throw new InvalidSubscriptionException(
                        "'#' 只能出现在 filter 的最后一层, filter=" + filter);
            }
        }
    }

    /**
     * 校验发布主题：与 filter 相同的层级约束，且不允许通配符。
     */
    public static void validatePublishTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new InvalidSubscriptionException("发布主题不能为空");
        }
        if (topic.length() > MAX_LENGTH) {
            throw new InvalidSubscriptionException(
                    "发布主题长度超过 " + MAX_LENGTH + " 字符: " + topic.length());
        }
        String[] segments = topic.split("/", -1);
        for (int i = 0; i < segments.length; i++) {
            String seg = segments[i];
            if (seg.isEmpty()) {
                throw new InvalidSubscriptionException(
                        "发布主题存在空层级（不允许前导/结尾/重复的 '/'），位置: 第 " + (i + 1)
                                + " 层, topic=" + topic);
            }
            if (containsWhitespace(seg)) {
                throw new InvalidSubscriptionException(
                        "发布主题的层级中不允许出现空白字符，位置: 第 " + (i + 1) + " 层, topic=" + topic);
            }
            if (seg.contains("+") || seg.contains("#")) {
                throw new InvalidSubscriptionException(
                        "发布主题不能包含通配符 '+' 或 '#'，位置: 第 " + (i + 1)
                                + " 层, topic=" + topic);
            }
        }
    }

    /**
     * 判断 filter 是否匹配具体主题。调用方需保证两个参数都已通过校验。
     */
    public static boolean matches(String filter, String topic) {
        String[] fs = filter.split("/", -1);
        String[] ts = topic.split("/", -1);
        int i = 0;
        for (; i < fs.length; i++) {
            String f = fs[i];
            if ("#".equals(f)) {
                // 只能在最后一层：匹配零层或任意多层后缀
                return true;
            }
            if (i >= ts.length) {
                return false;
            }
            if ("+".equals(f)) {
                continue; // 恰好一层，段非空已由校验保证
            }
            if (!f.equals(ts[i])) {
                return false;
            }
        }
        return i == ts.length;
    }

    private static boolean containsWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return true;
            }
        }
        return false;
    }
}
