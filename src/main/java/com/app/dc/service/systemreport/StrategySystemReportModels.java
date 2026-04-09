package com.app.dc.service.systemreport;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class StrategySystemReportModels {

    private StrategySystemReportModels() {
    }

    public static class CountRow {
        public String key1;
        public String key2;
        public Long total;
    }

    public static class ActiveLiveRow {
        public String strategyName;
        public String strategyVersion;
        public String scene;
        public String runtimeType;
        public String symbolScope;
        public String textScope;
        public String effectiveTime;
        public String description;
    }

    public static class ReviewFactRow {
        public String strategyName;
        public String strategyVersion;
        public String symbol;
        public String text;
        public String factType;
        public Integer severity;
        public String reviewReportPath;
        public String payload;
    }

    public static class ReportSummary {
        public String reportId;
        public String reportDate;
        public String generatedAt;
        public String since;
        public Map<String, Object> headline = new LinkedHashMap<String, Object>();
        public List<CountRow> rawCounts = new ArrayList<CountRow>();
        public List<CountRow> normCounts = new ArrayList<CountRow>();
        public List<CountRow> generationCounts = new ArrayList<CountRow>();
        public List<CountRow> backtestCounts = new ArrayList<CountRow>();
        public List<CountRow> optimizationCounts = new ArrayList<CountRow>();
        public List<CountRow> releaseCounts = new ArrayList<CountRow>();
        public List<CountRow> reviewFactCounts = new ArrayList<CountRow>();
        public List<ActiveLiveRow> activeLives = new ArrayList<ActiveLiveRow>();
        public List<ReviewFactRow> topReviewFacts = new ArrayList<ReviewFactRow>();
    }

    public static class ReportFiles {
        public String jsonPath;
        public String htmlPath;
    }

    public static class ReportItem {
        public String id;
        public String reportId;
        public String reportDate;
        public String section;
        public String itemKey;
        public String itemName;
        public Double metricValue;
        public String metricText;
        public String payload;
        public String createTime;
    }
}
