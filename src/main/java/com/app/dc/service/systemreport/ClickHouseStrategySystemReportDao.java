package com.app.dc.service.systemreport;

import com.app.common.db.ClickHouseDBUtils;
import com.app.dc.pipeline.ClickHouseStrategyPipelineDao;
import com.app.dc.pipeline.StrategyPipelineModels.PipelineBlockedRunRow;
import com.app.dc.pipeline.StrategyPipelineModels.PipelineCountRow;
import com.gateway.connector.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.app.dc.service.systemreport.StrategySystemReportModels.ActiveLiveRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.BlockedRunRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.CountRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.GenerationDetailRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.NormDetailRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.OptimizationDetailRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.PublishDetailRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.ReportItem;
import static com.app.dc.service.systemreport.StrategySystemReportModels.ReviewFactRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.ReportSummary;
import static com.app.dc.service.systemreport.StrategySystemReportModels.RuntimeStrategyRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.BacktestDetailRow;

@Component
@Slf4j
public class ClickHouseStrategySystemReportDao {

    @Autowired(required = false)
    private ClickHouseDBUtils clickHouseDBUtils;

    @Autowired(required = false)
    private ClickHouseStrategyPipelineDao strategyPipelineDao;

    @Value("${strategy.generation.task.table:dc.strategy_generation_task}")
    private String generationTaskTable;

    @Value("${strategy.backtest.task.table:dc.strategy_backtest_task}")
    private String backtestTaskTable;

    @Value("${binanceBacktestOptimizationTrialTable:dc.backtest_optimization_trial}")
    private String optimizationTrialTable;

    @Value("${strategy.release.event.table:dc.strategy_release_event}")
    private String releaseEventTable;

    @Value("${strategy.live.registry.table:dc.strategy_live_registry}")
    private String liveRegistryTable;

    @Value("${strategy.review.fact.table:dc.strategy_review_fact}")
    private String reviewFactTable;

    @Value("${strategy.pipeline.run.table:dc.strategy_pipeline_run}")
    private String pipelineRunTable;

    @Value("${signal.table:dc.signal}")
    private String signalTable;

    @Value("${quant.order.latest.view.table:dc.quant_order_latest_view}")
    private String quantOrderLatestViewTable;

    @Value("${quant.trade.latest.view.table:dc.quant_trade_latest_view}")
    private String quantTradeLatestViewTable;

    @Value("${strategy.system.daily.report.table:dc.strategy_system_daily_report}")
    private String reportTable;

    @Value("${strategy.system.daily.report.item.table:dc.strategy_system_daily_report_item}")
    private String reportItemTable;

    public boolean ready() {
        return clickHouseDBUtils != null && StringUtils.isNotBlank(clickHouseDBUtils.getDbSourceName());
    }

    public List<CountRow> countLatestGenerationBySourceAndStatusSince(String since) {
        return countLatestByStatus(safe(generationTaskTable, "dc.strategy_generation_task"),
                "id", "source_type", "status", "update_time", since);
    }

    public List<CountRow> countLatestBacktestByStatusSince(String since) {
        return countLatestByStatus(safe(backtestTaskTable, "dc.strategy_backtest_task"),
                "id", "task_type", "status", "update_time", since);
    }

    public List<CountRow> countOptimizationTrialsByPhaseSince(String since) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String sql = "select phase as key1, '' as key2, count() as total from "
                + safe(optimizationTrialTable, "dc.backtest_optimization_trial")
                + " where run_time >= toDateTime('" + escape(since) + "') group by phase order by total desc";
        return queryCountRows(sql);
    }

    public List<CountRow> countReleaseEventsByTypeSince(String since) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String sql = "select event_type as key1, '' as key2, count() as total from "
                + safe(releaseEventTable, "dc.strategy_release_event")
                + " where event_time >= toDateTime('" + escape(since) + "') group by event_type order by total desc";
        return queryCountRows(sql);
    }

    public List<ActiveLiveRow> loadCurrentActiveLives() {
        if (!ready()) {
            return Collections.emptyList();
        }
        String sql = "select "
                + "strategy_name as strategyName,"
                + "strategy_version as strategyVersion,"
                + "scene as scene,"
                + "runtime_type as runtimeType,"
                + "symbol_scope as symbolScope,"
                + "text_scope as textScope,"
                + "toString(effective_time) as effectiveTime,"
                + "description as description "
                + "from " + safe(liveRegistryTable, "dc.strategy_live_registry")
                + " where status='ACTIVE' and (retire_time is null or retire_time > now())"
                + " order by scene asc, strategy_name asc";
        try {
            List<ActiveLiveRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, ActiveLiveRow.class);
            return rows == null ? Collections.<ActiveLiveRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("loadCurrentActiveLives error", e);
            return Collections.emptyList();
        }
    }

    public List<CountRow> countReviewFactsBySeveritySince(String reportDate) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String endDate = nextDate(reportDate);
        String sql = "select toString(severity) as key1, fact_type as key2, count() as total from "
                + safe(reviewFactTable, "dc.strategy_review_fact")
                + " where trade_date >= toDate('" + escape(reportDate) + "')"
                + " and trade_date < toDate('" + escape(endDate) + "')"
                + " group by severity, fact_type order by severity desc, total desc";
        return queryCountRows(sql);
    }

    public List<CountRow> countPipelineByStageAndStatusSince(String since) {
        if (strategyPipelineDao == null || !strategyPipelineDao.ready()) {
            return Collections.emptyList();
        }
        List<PipelineCountRow> rows = strategyPipelineDao.countLatestRunsByStageAndStatusSince(since);
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<CountRow> result = new java.util.ArrayList<CountRow>();
        for (PipelineCountRow row : rows) {
            CountRow item = new CountRow();
            item.key1 = row.key1;
            item.key2 = row.key2;
            item.total = row.total;
            result.add(item);
        }
        return result;
    }

    public List<NormDetailRow> loadLatestNormDetailsSince(String since, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String table = "dc.strategy_source_norm";
        String inner = "select "
                + "id,"
                + "argMax(source_type, update_time) as sourceType,"
                + "argMax(site_name, update_time) as siteName,"
                + "argMax(strategy_name, update_time) as strategyName,"
                + "argMax(scene, update_time) as scene,"
                + "argMax(status, update_time) as status,"
                + "argMax(normalized_title, update_time) as normalizedTitle,"
                + "argMax(canonical_url, update_time) as canonicalUrl,"
                + "max(update_time) as updateTime "
                + "from " + table + " group by id";
        String sql = "select "
                + "latest.sourceType as sourceType,"
                + "latest.siteName as siteName,"
                + "latest.strategyName as strategyName,"
                + "'' as strategyVersion,"
                + "latest.scene as scene,"
                + "latest.status as status,"
                + "latest.normalizedTitle as normalizedTitle,"
                + "latest.canonicalUrl as canonicalUrl,"
                + "toString(latest.updateTime) as updateTime "
                + "from (" + inner + ") latest "
                + "where latest.updateTime >= toDateTime('" + escape(since) + "') "
                + "order by latest.updateTime desc limit " + Math.max(1, limit);
        try {
            List<NormDetailRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, NormDetailRow.class);
            return rows == null ? Collections.<NormDetailRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("loadLatestNormDetailsSince error, since:{}", since, e);
            return Collections.emptyList();
        }
    }

    public List<GenerationDetailRow> loadLatestGenerationDetailsSince(String since, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String inner = "select "
                + "id,"
                + "argMax(source_type, update_time) as sourceType,"
                + "argMax(strategy_name, update_time) as strategyName,"
                + "argMax(strategy_version, update_time) as strategyVersion,"
                + "argMax(scene, update_time) as scene,"
                + "argMax(status, update_time) as status,"
                + "argMax(compile_status, update_time) as compileStatus,"
                + "max(update_time) as updateTime "
                + "from " + safe(generationTaskTable, "dc.strategy_generation_task")
                + " group by id";
        String sql = "select "
                + "latest.sourceType as sourceType,"
                + "latest.strategyName as strategyName,"
                + "latest.strategyVersion as strategyVersion,"
                + "latest.scene as scene,"
                + "latest.status as status,"
                + "latest.compileStatus as compileStatus,"
                + "toString(latest.updateTime) as updateTime "
                + "from (" + inner + ") latest "
                + "where latest.updateTime >= toDateTime('" + escape(since) + "') "
                + "order by latest.updateTime desc limit " + Math.max(1, limit);
        try {
            List<GenerationDetailRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, GenerationDetailRow.class);
            return rows == null ? Collections.<GenerationDetailRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("loadLatestGenerationDetailsSince error, since:{}", since, e);
            return Collections.emptyList();
        }
    }

    public List<BacktestDetailRow> loadLatestBacktestDetailsSince(String since, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String inner = "select "
                + "id,"
                + "argMax(strategy_name, update_time) as strategyName,"
                + "argMax(strategy_version, update_time) as strategyVersion,"
                + "argMax(baseline_version, update_time) as baselineVersion,"
                + "argMax(task_type, update_time) as taskType,"
                + "argMax(status, update_time) as status,"
                + "argMax(suspend_reason, update_time) as suspendReason,"
                + "max(update_time) as updateTime "
                + "from " + safe(backtestTaskTable, "dc.strategy_backtest_task")
                + " group by id";
        String sql = "select "
                + "latest.strategyName as strategyName,"
                + "latest.strategyVersion as strategyVersion,"
                + "latest.baselineVersion as baselineVersion,"
                + "latest.taskType as taskType,"
                + "latest.status as status,"
                + "latest.suspendReason as suspendReason,"
                + "toString(latest.updateTime) as updateTime "
                + "from (" + inner + ") latest "
                + "where latest.updateTime >= toDateTime('" + escape(since) + "') "
                + "order by latest.updateTime desc limit " + Math.max(1, limit);
        try {
            List<BacktestDetailRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, BacktestDetailRow.class);
            return rows == null ? Collections.<BacktestDetailRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("loadLatestBacktestDetailsSince error, since:{}", since, e);
            return Collections.emptyList();
        }
    }

    public List<OptimizationDetailRow> loadOptimizationDetailsSince(String since, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String sql = "select "
                + "strategy_name as strategyName,"
                + "strategy_version as strategyVersion,"
                + "symbol as symbol,"
                + "text as text,"
                + "optimization_mode as optimizationMode,"
                + "toInt32(ifNull(trial_count, 0)) as trialCount,"
                + "toInt32(ifNull(best_rank, 0)) as bestRank,"
                + "best_param_set as bestParamSet,"
                + "toString(run_time) as runTime "
                + "from dc.backtest_result "
                + "where run_time >= toDateTime('" + escape(since) + "') "
                + "order by run_time desc limit " + Math.max(1, limit);
        try {
            List<OptimizationDetailRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, OptimizationDetailRow.class);
            return rows == null ? Collections.<OptimizationDetailRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("loadOptimizationDetailsSince error, since:{}", since, e);
            return Collections.emptyList();
        }
    }

    public List<PublishDetailRow> loadPublishDetailsSince(String since, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String sql = "select "
                + "strategy_name as strategyName,"
                + "from_version as fromVersion,"
                + "to_version as toVersion,"
                + "event_type as eventType,"
                + "reason as reason,"
                + "toString(event_time) as eventTime "
                + "from " + safe(releaseEventTable, "dc.strategy_release_event")
                + " where event_time >= toDateTime('" + escape(since) + "') "
                + "order by event_time desc limit " + Math.max(1, limit);
        try {
            List<PublishDetailRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, PublishDetailRow.class);
            return rows == null ? Collections.<PublishDetailRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("loadPublishDetailsSince error, since:{}", since, e);
            return Collections.emptyList();
        }
    }

    public List<BlockedRunRow> loadBlockedRunsSince(String since, int limit) {
        if (strategyPipelineDao == null || !strategyPipelineDao.ready()) {
            return Collections.emptyList();
        }
        List<PipelineBlockedRunRow> rows = strategyPipelineDao.loadLatestBlockedRunsSince(since, limit);
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        java.util.LinkedHashMap<String, BlockedRunRow> latest = new java.util.LinkedHashMap<String, BlockedRunRow>();
        for (PipelineBlockedRunRow row : rows) {
            if (StringUtils.equalsAnyIgnoreCase(row.currentReason, "DUPLICATE_RAW", "DUPLICATE_NORM")) {
                continue;
            }
            BlockedRunRow item = new BlockedRunRow();
            item.runId = row.runId;
            item.sourceType = row.sourceType;
            item.sourceRef = row.sourceRef;
            item.strategyName = row.strategyName;
            item.strategyVersion = row.strategyVersion;
            item.currentStage = row.currentStage;
            item.currentStatus = row.currentStatus;
            item.currentReason = row.currentReason;
            item.updateTime = row.updateTime;
            item.payload = row.payload;
            String key = safeValue(row.strategyName) + "|" + safeValue(row.strategyVersion) + "|" + safeValue(row.currentStage) + "|" + safeValue(row.currentReason);
            BlockedRunRow existing = latest.get(key);
            if (existing == null || (item.updateTime != null && item.updateTime.compareTo(existing.updateTime) > 0)) {
                latest.put(key, item);
            }
        }
        return new java.util.ArrayList<BlockedRunRow>(latest.values());
    }

    public List<RuntimeStrategyRow> loadRuntimeStrategyRows(String reportDate) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String endDate = nextDate(reportDate);
        String sql = "select strategyName as strategyName, strategyVersion as strategyVersion,"
                + " sum(signalCount) as signalCount,"
                + " sum(orderCount) as orderCount,"
                + " sum(tradeCount) as tradeCount,"
                + " round(toFloat64(sum(realizedPnl)), 8) as realizedPnl,"
                + " sum(errorCount) as errorCount"
                + " from ("
                + " select strategyName, strategyVersion, toInt64(count()) as signalCount, toInt64(0) as orderCount, toInt64(0) as tradeCount,"
                + " toFloat64(0) as realizedPnl, toInt64(0) as errorCount"
                + " from " + safe(signalTable, "dc.signal")
                + " where tradeDate >= toDate('" + escape(reportDate) + "')"
                + " and tradeDate < toDate('" + escape(endDate) + "')"
                + " and strategyName != ''"
                + " group by strategyName, strategyVersion"
                + " union all "
                + " select strategyName, strategyVersion, toInt64(0) as signalCount, toInt64(count()) as orderCount, toInt64(0) as tradeCount,"
                + " toFloat64(0) as realizedPnl, toInt64(0) as errorCount"
                + " from " + safe(quantOrderLatestViewTable, "dc.quant_order_latest_view")
                + " where tradeDate >= toDate('" + escape(reportDate) + "')"
                + " and tradeDate < toDate('" + escape(endDate) + "')"
                + " and strategyName != ''"
                + " and source in ('live', 'order_live')"
                + " group by strategyName, strategyVersion"
                + " union all "
                + " select strategyName, strategyVersion, toInt64(0) as signalCount, toInt64(0) as orderCount, toInt64(count()) as tradeCount,"
                + " toFloat64(sum(ifNull(realizedPnl, toDecimal64(0, 12)))) as realizedPnl, toInt64(0) as errorCount"
                + " from " + safe(quantTradeLatestViewTable, "dc.quant_trade_latest_view")
                + " where tradeDate >= toDate('" + escape(reportDate) + "')"
                + " and tradeDate < toDate('" + escape(endDate) + "')"
                + " and strategyName != ''"
                + " and source in ('live', 'order_live')"
                + " group by strategyName, strategyVersion"
                + " union all "
                + " select strategy_name as strategyName, strategy_version as strategyVersion,"
                + " toInt64(0) as signalCount, toInt64(0) as orderCount, toInt64(0) as tradeCount,"
                + " toFloat64(0) as realizedPnl, toInt64(count()) as errorCount"
                + " from " + safe(reviewFactTable, "dc.strategy_review_fact")
                + " where trade_date >= toDate('" + escape(reportDate) + "')"
                + " and trade_date < toDate('" + escape(endDate) + "')"
                + " and severity > 0 and strategy_name != ''"
                + " group by strategy_name, strategy_version"
                + " ) x group by strategyName, strategyVersion"
                + " order by realizedPnl desc, signalCount desc, strategyName asc";
        try {
            List<RuntimeStrategyRow> rows =
                    ClickHouseDBUtils.queryList(sql, new Object[]{}, RuntimeStrategyRow.class);
            return rows == null ? Collections.<RuntimeStrategyRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("loadRuntimeStrategyRows error, reportDate:{}", reportDate, e);
            return Collections.emptyList();
        }
    }

    public List<ReviewFactRow> loadTopReviewFacts(String reportDate, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String endDate = nextDate(reportDate);
        String sql = "select "
                + "strategy_name as strategyName,"
                + "strategy_version as strategyVersion,"
                + "symbol as symbol,"
                + "text as text,"
                + "fact_type as factType,"
                + "toInt32(severity) as severity,"
                + "review_report_path as reviewReportPath,"
                + "payload as payload "
                + "from " + safe(reviewFactTable, "dc.strategy_review_fact")
                + " where trade_date >= toDate('" + escape(reportDate) + "')"
                + " and trade_date < toDate('" + escape(endDate) + "')"
                + " order by severity desc, strategy_name asc limit " + Math.max(1, limit);
        try {
            List<ReviewFactRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, ReviewFactRow.class);
            return rows == null ? Collections.<ReviewFactRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("loadTopReviewFacts error, reportDate:{}", reportDate, e);
            return Collections.emptyList();
        }
    }

    public void insertReport(String id, String reportDate, String htmlPath, String jsonPath, ReportSummary summary) {
        if (!ready()) {
            return;
        }
        String sql = "insert into " + safe(reportTable, "dc.strategy_system_daily_report")
                + " (id, report_date, generated_at, summary_json, html_path, json_path, payload)"
                + " values (?,?,?,?,?,?,?)";
        try {
            String summaryJson = JsonUtils.Serializer(summary);
            ClickHouseDBUtils.update(sql, new Object[]{
                    id,
                    reportDate,
                    Timestamp.from(Instant.now()),
                    summaryJson,
                    htmlPath == null ? "" : htmlPath,
                    jsonPath == null ? "" : jsonPath,
                    summaryJson
            });
        } catch (Exception e) {
            log.error("insertReport error, reportDate:{}", reportDate, e);
        }
    }

    public void insertReportItems(List<ReportItem> items) {
        if (!ready() || items == null || items.isEmpty()) {
            return;
        }
        String sql = "insert into " + safe(reportItemTable, "dc.strategy_system_daily_report_item")
                + " (id, report_id, report_date, section, item_key, item_name, metric_value, metric_text, payload, create_time)"
                + " values (?,?,?,?,?,?,?,?,?,?)";
        for (ReportItem item : items) {
            try {
                ClickHouseDBUtils.update(sql, new Object[]{
                        item.id,
                        item.reportId,
                        item.reportDate,
                        item.section,
                        item.itemKey,
                        item.itemName,
                        item.metricValue == null ? 0D : item.metricValue,
                        item.metricText == null ? "" : item.metricText,
                        item.payload == null ? "" : item.payload,
                        Timestamp.from(Instant.now())
                });
            } catch (Exception e) {
                log.error("insertReportItems error, reportId:{}, itemKey:{}", item.reportId, item.itemKey, e);
            }
        }
    }

    public StrategySystemDailyReportRow loadLatestReport() {
        if (!ready()) {
            return null;
        }
        String sql = "select "
                + "id as id,"
                + "toString(report_date) as reportDate,"
                + "toString(generated_at) as generatedAt,"
                + "summary_json as summaryJson,"
                + "html_path as htmlPath,"
                + "json_path as jsonPath,"
                + "payload as payload "
                + "from " + safe(reportTable, "dc.strategy_system_daily_report")
                + " order by report_date desc, generated_at desc limit 1";
        List<StrategySystemDailyReportRow> rows = queryReportRows(sql);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public StrategySystemDailyReportRow loadReportByDate(String reportDate) {
        if (!ready() || StringUtils.isBlank(reportDate)) {
            return null;
        }
        String date = reportDate.trim();
        if (!date.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return null;
        }
        String sql = "select "
                + "id as id,"
                + "toString(report_date) as reportDate,"
                + "toString(generated_at) as generatedAt,"
                + "summary_json as summaryJson,"
                + "html_path as htmlPath,"
                + "json_path as jsonPath,"
                + "payload as payload "
                + "from " + safe(reportTable, "dc.strategy_system_daily_report")
                + " where report_date = toDate('" + escape(date) + "')"
                + " order by generated_at desc limit 1";
        List<StrategySystemDailyReportRow> rows = queryReportRows(sql);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<StrategySystemDailyReportRow> loadRecentReports(int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        int size = Math.max(1, limit);
        String sql = "select "
                + "id as id,"
                + "toString(report_date) as reportDate,"
                + "toString(generated_at) as generatedAt,"
                + "'' as summaryJson,"
                + "html_path as htmlPath,"
                + "json_path as jsonPath,"
                + "'' as payload "
                + "from " + safe(reportTable, "dc.strategy_system_daily_report")
                + " order by report_date desc, generated_at desc limit " + size;
        return queryReportRows(sql);
    }

    private List<CountRow> countLatestByStatus(String table,
                                               String idField,
                                               String key1Field,
                                               String key2Field,
                                               String timeField,
                                               String since) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String inner = "select " + idField + " as id, "
                + "argMax(" + key1Field + ", " + timeField + ") as key1, "
                + "argMax(" + key2Field + ", " + timeField + ") as key2, "
                + "max(" + timeField + ") as latestTime "
                + "from " + table + " group by " + idField;
        String sql = "select key1 as key1, key2 as key2, count() as total from (" + inner + ") latest "
                + "where latest.latestTime >= toDateTime('" + escape(since) + "') "
                + "group by key1, key2 order by key1 asc, key2 asc";
        return queryCountRows(sql);
    }

    private List<CountRow> queryCountRows(String sql) {
        try {
            List<CountRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, CountRow.class);
            return rows == null ? Collections.<CountRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("queryCountRows error, sql:{}", sql, e);
            return Collections.emptyList();
        }
    }

    private List<StrategySystemDailyReportRow> queryReportRows(String sql) {
        try {
            List<StrategySystemDailyReportRow> rows =
                    ClickHouseDBUtils.queryList(sql, new Object[]{}, StrategySystemDailyReportRow.class);
            return rows == null ? Collections.<StrategySystemDailyReportRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("queryReportRows error, sql:{}", sql, e);
            return Collections.emptyList();
        }
    }

    private String safe(String value, String fallback) {
        if (StringUtils.isBlank(value)) {
            return fallback;
        }
        String trim = value.trim();
        if (!trim.matches("[A-Za-z0-9_.]+")) {
            return fallback;
        }
        return trim;
    }

    private String safeValue(String text) {
        return text == null ? "" : text;
    }

    private String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("'", "''");
    }

 
    public static class StrategySystemDailyReportRow {
        public String id;
        public String reportDate;
        public String generatedAt;
        public String summaryJson;
        public String htmlPath;
        public String jsonPath;
        public String payload;
    }
    private String nextDate(String date) {
        try {
            return java.time.LocalDate.parse(date).plusDays(1).toString();
        } catch (Exception e) {
            return date;
        }
 
    }
   
}
