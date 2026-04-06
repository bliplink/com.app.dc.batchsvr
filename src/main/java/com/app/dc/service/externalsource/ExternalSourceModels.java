package com.app.dc.service.externalsource;

public class ExternalSourceModels {

    public enum SourceType {
        TRADINGVIEW("TradingView"),
        FMZ("FMZ"),
        GITHUB("GitHub");

        private final String siteName;

        SourceType(String siteName) {
            this.siteName = siteName;
        }

        public String siteName() {
            return siteName;
        }
    }

    public static class RawRecord {
        public String id;
        public String sourceType;
        public String siteName;
        public String sourceName;
        public String externalId;
        public String canonicalUrl;
        public String sourceUrl;
        public String title;
        public String author;
        public String publishedTime;
        public String externalUpdateTime;
        public String contentHash;
        public String content;
        public String status;
        public String createTime;
        public String payload;
    }

    public static class NormRecord {
        public String id;
        public String rawId;
        public String sourceType;
        public String siteName;
        public String canonicalUrl;
        public String normalizedTitle;
        public String scene;
        public String strategyName;
        public String description;
        public String content;
        public String rawContentHash;
        public String fingerprint;
        public String status;
        public String createTime;
        public String updateTime;
        public String payload;
    }

    public static class CountRow {
        public String key1;
        public String key2;
        public Long total;
    }
}
