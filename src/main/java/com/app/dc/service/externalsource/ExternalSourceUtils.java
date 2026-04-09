package com.app.dc.service.externalsource;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.Normalizer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public final class ExternalSourceUtils {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private ExternalSourceUtils() {
    }

    public static String nowText() {
        return LocalDateTime.now().format(TS);
    }

    public static String safe(String value) {
        if (value == null) {
            return "";
        }
        return value.replace('\u00A0', ' ').replaceAll("\\s+", " ").trim();
    }

    public static String truncate(String value, int maxLen) {
        String text = safe(value);
        if (text.length() <= maxLen) {
            return text;
        }
        return text.substring(0, Math.max(0, maxLen)) + "...";
    }

    public static String slug(String input, String fallback) {
        String text = safe(input).toLowerCase(Locale.ROOT);
        if (StringUtils.isBlank(text)) {
            return fallback;
        }
        text = Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        text = text.replaceAll("[^a-z0-9]+", "-").replaceAll("(^-|-$)", "");
        if (StringUtils.isBlank(text)) {
            return fallback;
        }
        text = truncate(text, 64).replace("...", "");
        return StringUtils.isBlank(text) ? fallback : text;
    }

    public static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(safe(input).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return String.valueOf(Math.abs(safe(input).hashCode()));
        }
    }

    public static boolean containsAny(String text, String... tokens) {
        String lower = safe(text).toLowerCase(Locale.ROOT);
        for (String token : tokens) {
            if (lower.contains(token)) {
                return true;
            }
        }
        return false;
    }

    public static boolean looksCrypto(String title, String content) {
        return containsAny(title + " " + content,
                "crypto", "cryptocurrency", "bitcoin", "btc", "eth", "sol", "usdt", "binance", "altcoin", "futures", "perpetual");
    }

    public static String detectScene(String title, String content) {
        String text = title + " " + content;
        if (containsAny(text, "range", "mean reversion", "channel", "sideways", "consolidation")) {
            return "range";
        }
        if (containsAny(text, "reversal", "divergence", "rejection", "failed break")) {
            return "reversal";
        }
        if (containsAny(text, "breakout", "squeeze", "compression", "volatility expansion")) {
            return "breakout";
        }
        if (containsAny(text, "trend", "momentum", "moving average", "continuation", "ema", "macd")) {
            return "trend";
        }
        return "trend";
    }

    public static String summarizeLogic(String content) {
        if (containsAny(content, "breakout", "squeeze", "compression")) {
            return "突破、波动压缩与放量确认";
        }
        if (containsAny(content, "mean reversion", "range", "channel", "sideways")) {
            return "区间震荡、均值回归与通道反复";
        }
        if (containsAny(content, "reversal", "divergence", "rejection")) {
            return "反转、背离与失败突破回补";
        }
        if (containsAny(content, "trend", "momentum", "ema", "moving average", "macd")) {
            return "趋势延续、动量确认与均线过滤";
        }
        return "价格行为、趋势判断与风控约束";
    }

    public static String buildChineseDescription(String siteName, String title, String scene, String content) {
        return String.format("%s 公开策略来源《%s》，推测适用于 %s 场景，核心逻辑包含 %s，建议先走回测验证后再决定是否进入候选策略链。",
                safe(siteName), safe(title), safe(scene), summarizeLogic(content));
    }
}
