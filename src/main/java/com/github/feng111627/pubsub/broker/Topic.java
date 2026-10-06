package com.github.feng111627.pubsub.broker;

import java.util.List;

/**
 * 层级式主题 / 主题过滤器。
 * 段以 '/' 分隔；过滤器支持 '+'（恰好匹配一段）与 '#'（匹配剩余任意多段，只能出现在末段）。
 * 发布主题不允许包含通配符。
 */
public record Topic(List<String> segments, boolean filter) {

    public static Topic parseFilter(String raw) {
        List<String> segs = split(raw);
        for (int i = 0; i < segs.size(); i++) {
            String s = segs.get(i);
            if (s.contains("#") && !s.equals("#")) {
                throw invalid(raw, "'#' 必须独占一段");
            }
            if (s.contains("+") && !s.equals("+")) {
                throw invalid(raw, "'+' 必须独占一段");
            }
            if (s.equals("#") && i != segs.size() - 1) {
                throw invalid(raw, "'#' 只能出现在最后一段");
            }
        }
        return new Topic(segs, true);
    }

    public static Topic parsePublishTopic(String raw) {
        List<String> segs = split(raw);
        for (String s : segs) {
            if (s.contains("#") || s.contains("+")) {
                throw invalid(raw, "发布主题不允许包含通配符 '+' 或 '#'");
            }
        }
        return new Topic(segs, false);
    }

    /** 本对象作为过滤器，判断给定发布主题是否匹配。 */
    public boolean matches(Topic publishTopic) {
        if (!filter) {
            throw new IllegalStateException("matches 只能在过滤器上调用");
        }
        List<String> f = segments;
        List<String> t = publishTopic.segments();
        for (int i = 0; i < f.size(); i++) {
            String fs = f.get(i);
            if (fs.equals("#")) {
                return true; // 末段 '#' 匹配剩余任意段（含零段）
            }
            if (i >= t.size()) {
                return false;
            }
            if (!fs.equals("+") && !fs.equals(t.get(i))) {
                return false;
            }
        }
        return f.size() == t.size();
    }

    private static List<String> split(String raw) {
        if (raw == null || raw.isBlank()) {
            throw invalid(String.valueOf(raw), "主题不能为空");
        }
        String trimmed = raw.trim();
        List<String> segs = List.of(trimmed.split("/", -1));
        for (String s : segs) {
            if (s.isEmpty()) {
                throw invalid(raw, "主题段不能为空（检查是否出现 '//' 或首尾 '/'）");
            }
        }
        return segs;
    }

    private static BrokerException invalid(String raw, String why) {
        return new BrokerException(BrokerException.Code.INVALID_TOPIC,
                "非法主题 '" + raw + "'：" + why);
    }

    @Override
    public String toString() {
        return String.join("/", segments);
    }
}
