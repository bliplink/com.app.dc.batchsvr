package com.app.dc.service.systemreport;

import com.app.common.db.ClickHouseDBUtils;
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
import static com.app.dc.service.systemreport.StrategySystemReportModels.CountRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.ReportItem;
import static com.app.dc.service.systemreport.StrategySystemReportModels.ReviewFactRow;
import static com.app.dc.service.systemreport.StrategySystemReportModels.ReportSummary;

@Component
@Slf4j
public class ClickHouseStrategySystemReportDao {

    @Autowired(required = false)
    private ClickHouseDBUtils clickHouseDBUtils;

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
        String sql = "select toString(severity) as key1, fact_type as key2, count() as total from "
                + safe(reviewFactTable, "dc.strategy_review_fact")
                + " where trade_date >= toDate('" + escape(reportDate) + "') group by severity, fact_type order by severity desc, total desc";
        return queryCountRows(sql);
    }

    public List<ReviewFactRow> loadTopReviewFacts(String reportDate, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
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

    private String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("'", "''");
    }
}
