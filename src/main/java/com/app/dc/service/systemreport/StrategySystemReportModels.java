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

    public static class NormDetailRow {
        public String sourceType;
        public String siteName;
        public String strategyName;
        public String strategyVersion;
        public String scene;
        public String status;
        public String normalizedTitle;
        public String canonicalUrl;
        public String updateTime;
    }

    public static class GenerationDetailRow {
        public String sourceType;
        public String strategyName;
        public String strategyVersion;
        public String scene;
        public String status;
        public String compileStatus;
        public String updateTime;
    }

    public static class BacktestDetailRow {
        public String strategyName;
        public String strategyVersion;
        public String baselineVersion;
        public String taskType;
        public String status;
        public String suspendReason;
        public String updateTime;
    }

    public static class OptimizationDetailRow {
        public String strategyName;
        public String strategyVersion;
        public String symbol;
        public String text;
        public String optimizationMode;
        public Integer trialCount;
        public Integer bestRank;
        public String bestParamSet;
        public String runTime;
    }

    public static class PublishDetailRow {
        public String strategyName;
        public String fromVersion;
        public String toVersion;
        public String eventType;
        public String reason;
        public String eventTime;
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

    public static class BlockedRunRow {
        public String runId;
        public String sourceType;
        public String sourceRef;
        public String strategyName;
        public String strategyVersion;
        public String currentStage;
        public String currentStatus;
        public String currentReason;
        public String updateTime;
        public String payload;
    }

    public static class RuntimeStrategyRow {
        public String strategyName;
        public String strategyVersion;
        public Long signalCount;
        public Long orderCount;
        public Long tradeCount;
        public Double realizedPnl;
        public Long errorCount;
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
        public List<CountRow> pipelineCounts = new ArrayList<CountRow>();
        public List<CountRow> reviewFactCounts = new ArrayList<CountRow>();
        public List<NormDetailRow> normDetails = new ArrayList<NormDetailRow>();
        public List<GenerationDetailRow> generationDetails = new ArrayList<GenerationDetailRow>();
        public List<BacktestDetailRow> backtestDetails = new ArrayList<BacktestDetailRow>();
        public List<OptimizationDetailRow> optimizationDetails = new ArrayList<OptimizationDetailRow>();
        public List<PublishDetailRow> publishDetails = new ArrayList<PublishDetailRow>();
        public List<ActiveLiveRow> activeLives = new ArrayList<ActiveLiveRow>();
        public List<BlockedRunRow> blockedRuns = new ArrayList<BlockedRunRow>();
        public List<RuntimeStrategyRow> runtimeRows = new ArrayList<RuntimeStrategyRow>();
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
