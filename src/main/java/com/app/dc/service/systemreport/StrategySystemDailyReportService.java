package com.app.dc.service.systemreport;

import com.app.common.utils.JsonUtils;
import com.app.dc.service.externalsource.ClickHouseExternalSourceDao;
import com.app.dc.service.systemreport.StrategySystemReportModels.ActiveLiveRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.BacktestDetailRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.BlockedRunRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.CountRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.GenerationDetailRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.NormDetailRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.OptimizationDetailRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.PublishDetailRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReportFiles;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReportItem;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReportSummary;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReviewFactRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.RuntimeStrategyRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@Service
@Slf4j
public class StrategySystemDailyReportService {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private ClickHouseExternalSourceDao externalSourceDao;

    @Autowired
    private ClickHouseStrategySystemReportDao systemReportDao;

    @Value("${strategy.system.report.enabled:true}")
    private boolean enabled;

    @Value("${strategy.system.report.reportDir:../../log/BatchSvr/strategy-system-reports}")
    private String reportDir;

    @Value("${strategy.system.report.detailLimit:50}")
    private int detailLimit;

    public String generateDailyReport() {
        if (!enabled || !systemReportDao.ready()) {
            return "";
        }
        LocalDate reportDate = LocalDate.now().minusDays(1);
        String since = reportDate.atStartOfDay().format(TS);
        String reportId = "system_report_" + reportDate.toString().replace("-", "");

        ReportSummary summary = new ReportSummary();
        summary.reportId = reportId;
        summary.reportDate = reportDate.toString();
        summary.generatedAt = LocalDateTime.now().format(TS);
        summary.since = since;
        summary.rawCounts.addAll(convertExternal(externalSourceDao.countLatestRawBySiteAndStatusSince(since)));
        summary.normCounts.addAll(convertExternal(externalSourceDao.countLatestNormBySiteAndStatusSince(since)));
        summary.generationCounts.addAll(systemReportDao.countLatestGenerationBySourceAndStatusSince(since));
        summary.backtestCounts.addAll(systemReportDao.countLatestBacktestByStatusSince(since));
        summary.optimizationCounts.addAll(systemReportDao.countOptimizationTrialsByPhaseSince(since));
        summary.releaseCounts.addAll(systemReportDao.countReleaseEventsByTypeSince(since));
        summary.pipelineCounts.addAll(systemReportDao.countPipelineByStageAndStatusSince(since));
        summary.reviewFactCounts.addAll(systemReportDao.countReviewFactsBySeveritySince(reportDate.toString()));
        summary.normDetails.addAll(systemReportDao.loadLatestNormDetailsSince(since, detailLimit));
        summary.generationDetails.addAll(systemReportDao.loadLatestGenerationDetailsSince(since, detailLimit));
        summary.backtestDetails.addAll(systemReportDao.loadLatestBacktestDetailsSince(since, detailLimit));
        summary.optimizationDetails.addAll(systemReportDao.loadOptimizationDetailsSince(since, detailLimit));
        summary.publishDetails.addAll(systemReportDao.loadPublishDetailsSince(since, detailLimit));
        summary.activeLives.addAll(systemReportDao.loadCurrentActiveLives());
        summary.blockedRuns.addAll(systemReportDao.loadBlockedRunsSince(since, 30));
        summary.runtimeRows.addAll(systemReportDao.loadRuntimeStrategyRows(reportDate.toString()));
        summary.topReviewFacts.addAll(systemReportDao.loadTopReviewFacts(reportDate.toString(), 20));

        summary.headline = new LinkedHashMap<String, Object>();
        summary.headline.put("rawTotal", sum(summary.rawCounts));
        summary.headline.put("readyNormTotal", sumByStatus(summary.normCounts, "READY"));
        summary.headline.put("generatedSuccessTotal", sumByStatus(summary.generationCounts, "SUCCESS"));
        summary.headline.put("backtestSuccessTotal", sumByStatus(summary.backtestCounts, "SUCCESS"));
        summary.headline.put("optimizationTrialTotal", sum(summary.optimizationCounts));
        summary.headline.put("publishEventTotal", sum(summary.releaseCounts));
        summary.headline.put("pipelineBlockedTotal", summary.blockedRuns.size());
        summary.headline.put("activeLiveTotal", summary.activeLives.size());
        summary.headline.put("runtimeSignalTotal", sumRuntime(summary.runtimeRows, "signal"));
        summary.headline.put("runtimeOrderTotal", sumRuntime(summary.runtimeRows, "order"));
        summary.headline.put("runtimeTradeTotal", sumRuntime(summary.runtimeRows, "trade"));
        summary.headline.put("runtimePnlTotal", scale(sumRuntimePnl(summary.runtimeRows)));
        summary.headline.put("runtimeErrorTotal", sumRuntime(summary.runtimeRows, "error"));

        ReportFiles files = writeFiles(reportId, summary);
        systemReportDao.insertReport(reportId, summary.reportDate, files.htmlPath, files.jsonPath, summary);
        systemReportDao.insertReportItems(buildItems(summary));
        return files.htmlPath;
    }

    private ReportFiles writeFiles(String reportId, ReportSummary summary) {
        ReportFiles files = new ReportFiles();
        try {
            Path dayDir = Paths.get(reportDir).resolve(summary.reportDate);
            Files.createDirectories(dayDir);
            files.jsonPath = dayDir.resolve(reportId + ".json").toString();
            files.htmlPath = dayDir.resolve(reportId + ".html").toString();
            Files.write(Paths.get(files.jsonPath), JsonUtils.Serializer(summary).getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(files.htmlPath), buildHtml(summary).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("StrategySystemDailyReportService writeFiles error, reportId:{}", reportId, e);
        }
        return files;
    }

    private String buildHtml(ReportSummary summary) {
        StringBuilder html = new StringBuilder();
        html.append("<html><head><meta charset=\"UTF-8\"><title>系统日报</title>")
                .append("<style>")
                .append("body{font-family:Segoe UI,Microsoft YaHei,sans-serif;margin:24px;color:#111827;background:#f8fafc;}")
                .append("h1,h2{margin:0 0 12px 0;}h2{margin-top:28px;}")
                .append(".muted{color:#6b7280;font-size:13px;}")
                .append(".cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:16px 0 24px 0;}")
                .append(".card{background:#fff;border:1px solid #d1d5db;border-radius:10px;padding:12px 14px;}")
                .append(".label{font-size:12px;color:#6b7280;}.value{font-size:24px;font-weight:700;margin-top:4px;}")
                .append("table{border-collapse:collapse;width:100%;margin:12px 0 24px 0;background:#fff;}")
                .append("th,td{border:1px solid #d1d5db;padding:8px 10px;font-size:13px;text-align:left;vertical-align:top;}")
                .append("th{background:#f3f4f6;}.mono{font-family:Consolas,Menlo,monospace;font-size:12px;}")
                .append(".ok{color:#047857;font-weight:600;}.bad{color:#b91c1c;font-weight:600;}.warn{color:#b45309;font-weight:600;}")
                .append("</style></head><body>");
        html.append("<h1>系统日报</h1>");
        html.append("<div class='muted'>报告日期: ").append(escape(summary.reportDate))
                .append(" / 生成时间: ").append(escape(summary.generatedAt))
                .append(" / 统计起点: ").append(escape(summary.since)).append("</div>");
        html.append("<div class='cards'>");
        appendCard(html, "采集 Raw", summary.headline.get("rawTotal"));
        appendCard(html, "待生成 Norm", summary.headline.get("readyNormTotal"));
        appendCard(html, "生成成功", summary.headline.get("generatedSuccessTotal"));
        appendCard(html, "回测成功", summary.headline.get("backtestSuccessTotal"));
        appendCard(html, "优化 Trial", summary.headline.get("optimizationTrialTotal"));
        appendCard(html, "发布事件", summary.headline.get("publishEventTotal"));
        appendCard(html, "当前 ACTIVE", summary.headline.get("activeLiveTotal"));
        appendCard(html, "当前卡点", summary.headline.get("pipelineBlockedTotal"));
        appendCard(html, "实盘信号", summary.headline.get("runtimeSignalTotal"));
        appendCard(html, "实盘订单", summary.headline.get("runtimeOrderTotal"));
        appendCard(html, "实盘成交", summary.headline.get("runtimeTradeTotal"));
        appendCard(html, "实盘已实现 PnL", summary.headline.get("runtimePnlTotal"));
        html.append("</div>");

        appendCountTable(html, "1. 采集阶段 Raw", summary.rawCounts, "来源", "状态", "数量");
        appendCountTable(html, "1. 采集阶段 Norm", summary.normCounts, "来源", "状态", "数量");
        appendNormDetails(html, summary.normDetails);
        appendCountTable(html, "2. 生成阶段统计", summary.generationCounts, "来源类型", "状态", "数量");
        appendGenerationDetails(html, summary.generationDetails);
        appendCountTable(html, "3. 回测阶段统计", summary.backtestCounts, "任务类型", "状态", "数量");
        appendBacktestDetails(html, summary.backtestDetails);
        appendCountTable(html, "4. 参数优化阶段统计", summary.optimizationCounts, "阶段", "状态", "数量");
        appendOptimizationDetails(html, summary.optimizationDetails);
        appendCountTable(html, "5. 发布阶段统计", summary.releaseCounts, "事件类型", "状态", "数量");
        appendPublishDetails(html, summary.publishDetails);
        appendCountTable(html, "流水线当前状态", summary.pipelineCounts, "阶段", "状态", "数量");
        appendActiveLives(html, summary.activeLives);
        appendRuntimeRows(html, summary.runtimeRows);
        appendBlockedRuns(html, summary.blockedRuns);
        appendCountTable(html, "实盘复盘异常统计", summary.reviewFactCounts, "严重级别", "事实类型", "数量");
        appendReviewFacts(html, summary.topReviewFacts);
        html.append("</body></html>");
        return html.toString();
    }

    private void appendCard(StringBuilder html, String title, Object value) {
        html.append("<div class='card'><div class='label'>").append(escape(title))
                .append("</div><div class='value'>").append(escape(String.valueOf(value))).append("</div></div>");
    }

    private void appendCountTable(StringBuilder html,
                                  String title,
                                  List<CountRow> rows,
                                  String key1Label,
                                  String key2Label,
                                  String valueLabel) {
        html.append("<h2>").append(escape(title)).append("</h2><table><tr><th>")
                .append(escape(key1Label)).append("</th><th>")
                .append(escape(key2Label)).append("</th><th>")
                .append(escape(valueLabel)).append("</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='3'>无数据</td></tr>");
        } else {
            for (CountRow row : rows) {
                html.append("<tr><td>").append(escape(row.key1)).append("</td><td>")
                        .append(escape(row.key2)).append("</td><td>")
                        .append(row.total == null ? 0L : row.total).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendNormDetails(StringBuilder html, List<NormDetailRow> rows) {
        html.append("<h2>1. 采集归一化策略池明细</h2><table><tr><th>来源</th><th>站点</th><th>策略名</th><th>版本</th><th>场景</th><th>状态</th><th>标题</th><th>更新时间</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='8'>无数据</td></tr>");
        } else {
            for (NormDetailRow row : rows) {
                html.append("<tr><td>").append(escape(row.sourceType)).append("</td><td>")
                        .append(escape(row.siteName)).append("</td><td>")
                        .append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.strategyVersion)).append("</td><td>")
                        .append(escape(row.scene)).append("</td><td>")
                        .append(escape(row.status)).append("</td><td>")
                        .append(escape(row.normalizedTitle)).append("</td><td>")
                        .append(escape(row.updateTime)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendGenerationDetails(StringBuilder html, List<GenerationDetailRow> rows) {
        html.append("<h2>2. 生成阶段明细</h2><table><tr><th>来源</th><th>策略名</th><th>版本</th><th>场景</th><th>生成状态</th><th>编译状态</th><th>更新时间</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>无数据</td></tr>");
        } else {
            for (GenerationDetailRow row : rows) {
                html.append("<tr><td>").append(escape(row.sourceType)).append("</td><td>")
                        .append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.strategyVersion)).append("</td><td>")
                        .append(escape(row.scene)).append("</td><td class='").append(cssStatus(row.status)).append("'>")
                        .append(escape(row.status)).append("</td><td>")
                        .append(escape(row.compileStatus)).append("</td><td>")
                        .append(escape(row.updateTime)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendBacktestDetails(StringBuilder html, List<BacktestDetailRow> rows) {
        html.append("<h2>3. 回测阶段明细</h2><table><tr><th>策略名</th><th>版本</th><th>基线版本</th><th>任务类型</th><th>状态</th><th>挂起原因</th><th>更新时间</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>无数据</td></tr>");
        } else {
            for (BacktestDetailRow row : rows) {
                html.append("<tr><td>").append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.strategyVersion)).append("</td><td>")
                        .append(escape(row.baselineVersion)).append("</td><td>")
                        .append(escape(row.taskType)).append("</td><td class='").append(cssStatus(row.status)).append("'>")
                        .append(escape(row.status)).append("</td><td>")
                        .append(escape(row.suspendReason)).append("</td><td>")
                        .append(escape(row.updateTime)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendOptimizationDetails(StringBuilder html, List<OptimizationDetailRow> rows) {
        html.append("<h2>4. 参数优化阶段明细</h2><table><tr><th>策略名</th><th>版本</th><th>标的</th><th>周期</th><th>模式</th><th>Trial 数</th><th>最佳排名</th><th>最优参数</th><th>运行时间</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='9'>无数据</td></tr>");
        } else {
            for (OptimizationDetailRow row : rows) {
                html.append("<tr><td>").append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.strategyVersion)).append("</td><td>")
                        .append(escape(row.symbol)).append("</td><td>")
                        .append(escape(row.text)).append("</td><td>")
                        .append(escape(row.optimizationMode)).append("</td><td>")
                        .append(row.trialCount == null ? 0 : row.trialCount).append("</td><td>")
                        .append(row.bestRank == null ? 0 : row.bestRank).append("</td><td class='mono'>")
                        .append(escape(row.bestParamSet)).append("</td><td>")
                        .append(escape(row.runTime)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendPublishDetails(StringBuilder html, List<PublishDetailRow> rows) {
        html.append("<h2>5. 发布阶段明细</h2><table><tr><th>策略名</th><th>旧版本</th><th>新版本</th><th>事件</th><th>原因</th><th>时间</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='6'>无数据</td></tr>");
        } else {
            for (PublishDetailRow row : rows) {
                html.append("<tr><td>").append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.fromVersion)).append("</td><td>")
                        .append(escape(row.toVersion)).append("</td><td>")
                        .append(escape(row.eventType)).append("</td><td>")
                        .append(escape(row.reason)).append("</td><td>")
                        .append(escape(row.eventTime)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendActiveLives(StringBuilder html, List<ActiveLiveRow> rows) {
        html.append("<h2>6. 当前 ACTIVE 清单</h2><table><tr><th>策略</th><th>版本</th><th>场景</th><th>运行类型</th><th>Symbol</th><th>周期</th><th>生效时间</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>当前没有 ACTIVE 策略</td></tr>");
        } else {
            for (ActiveLiveRow row : rows) {
                html.append("<tr><td>").append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.strategyVersion)).append("</td><td>")
                        .append(escape(row.scene)).append("</td><td>")
                        .append(escape(row.runtimeType)).append("</td><td>")
                        .append(escape(row.symbolScope)).append("</td><td>")
                        .append(escape(row.textScope)).append("</td><td>")
                        .append(escape(row.effectiveTime)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendRuntimeRows(StringBuilder html, List<RuntimeStrategyRow> rows) {
        html.append("<h2>6. 实盘运行摘要</h2><table><tr><th>策略</th><th>版本</th><th>信号数</th><th>订单数</th><th>成交数</th><th>已实现 PnL</th><th>异常数</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>今日暂无实盘运行数据</td></tr>");
        } else {
            for (RuntimeStrategyRow row : rows) {
                html.append("<tr><td>").append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.strategyVersion)).append("</td><td>")
                        .append(nz(row.signalCount)).append("</td><td>")
                        .append(nz(row.orderCount)).append("</td><td>")
                        .append(nz(row.tradeCount)).append("</td><td>")
                        .append(scale(row.realizedPnl)).append("</td><td>")
                        .append(nz(row.errorCount)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendBlockedRuns(StringBuilder html, List<BlockedRunRow> rows) {
        html.append("<h2>当前卡点策略</h2><table><tr><th>runId</th><th>来源</th><th>策略</th><th>版本</th><th>当前阶段</th><th>状态</th><th>原因</th><th>更新时间</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='8'>当前没有 FAILED / SKIPPED / SUSPENDED 的流水线</td></tr>");
        } else {
            for (BlockedRunRow row : rows) {
                html.append("<tr><td>").append(escape(row.runId)).append("</td><td>")
                        .append(escape(row.sourceType)).append("</td><td>")
                        .append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.strategyVersion)).append("</td><td>")
                        .append(escape(row.currentStage)).append("</td><td class='")
                        .append(cssStatus(row.currentStatus)).append("'>")
                        .append(escape(row.currentStatus)).append("</td><td>")
                        .append(escape(row.currentReason)).append("</td><td>")
                        .append(escape(row.updateTime)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private void appendReviewFacts(StringBuilder html, List<ReviewFactRow> rows) {
        html.append("<h2>重点实盘复盘事实</h2><table><tr><th>策略</th><th>版本</th><th>Symbol</th><th>周期</th><th>Fact</th><th>Severity</th><th>报告路径</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>今日暂无高优先级复盘事实</td></tr>");
        } else {
            for (ReviewFactRow row : rows) {
                html.append("<tr><td>").append(escape(row.strategyName)).append("</td><td>")
                        .append(escape(row.strategyVersion)).append("</td><td>")
                        .append(escape(row.symbol)).append("</td><td>")
                        .append(escape(row.text)).append("</td><td>")
                        .append(escape(row.factType)).append("</td><td>")
                        .append(row.severity == null ? 0 : row.severity).append("</td><td>")
                        .append(escape(row.reviewReportPath)).append("</td></tr>");
            }
        }
        html.append("</table>");
    }

    private List<ReportItem> buildItems(ReportSummary summary) {
        List<ReportItem> items = new ArrayList<ReportItem>();
        addCountItems(items, summary.reportId, summary.reportDate, "discover.raw", summary.rawCounts);
        addCountItems(items, summary.reportId, summary.reportDate, "discover.norm", summary.normCounts);
        addCountItems(items, summary.reportId, summary.reportDate, "generate", summary.generationCounts);
        addCountItems(items, summary.reportId, summary.reportDate, "backtest", summary.backtestCounts);
        addCountItems(items, summary.reportId, summary.reportDate, "optimization", summary.optimizationCounts);
        addCountItems(items, summary.reportId, summary.reportDate, "publish", summary.releaseCounts);
        addCountItems(items, summary.reportId, summary.reportDate, "pipeline", summary.pipelineCounts);
        addCountItems(items, summary.reportId, summary.reportDate, "review_fact", summary.reviewFactCounts);

        for (NormDetailRow row : summary.normDetails) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "discover.norm.detail");
            item.itemKey = safe(row.strategyName) + "@" + safe(row.strategyVersion);
            item.itemName = safe(row.strategyName);
            item.metricValue = 1D;
            item.metricText = safe(row.status) + " / " + safe(row.scene);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
        for (GenerationDetailRow row : summary.generationDetails) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "generate.detail");
            item.itemKey = safe(row.strategyName) + "@" + safe(row.strategyVersion);
            item.itemName = safe(row.strategyName);
            item.metricValue = 1D;
            item.metricText = safe(row.status) + " / compile=" + safe(row.compileStatus);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
        for (BacktestDetailRow row : summary.backtestDetails) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "backtest.detail");
            item.itemKey = safe(row.strategyName) + "@" + safe(row.strategyVersion);
            item.itemName = safe(row.strategyName);
            item.metricValue = 1D;
            item.metricText = safe(row.status) + " / " + safe(row.taskType);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
        for (OptimizationDetailRow row : summary.optimizationDetails) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "optimization.detail");
            item.itemKey = safe(row.strategyName) + "@" + safe(row.strategyVersion);
            item.itemName = safe(row.strategyName);
            item.metricValue = row.bestRank == null ? 0D : row.bestRank.doubleValue();
            item.metricText = "trials=" + (row.trialCount == null ? 0 : row.trialCount);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
        for (PublishDetailRow row : summary.publishDetails) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "publish.detail");
            item.itemKey = safe(row.strategyName) + "@" + safe(row.toVersion);
            item.itemName = safe(row.strategyName);
            item.metricValue = 1D;
            item.metricText = safe(row.eventType) + " / " + safe(row.reason);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
        for (ActiveLiveRow row : summary.activeLives) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "live.active");
            item.itemKey = safe(row.strategyName) + "@" + safe(row.strategyVersion);
            item.itemName = safe(row.scene);
            item.metricValue = 1D;
            item.metricText = safe(row.symbolScope) + " / " + safe(row.textScope);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
        for (RuntimeStrategyRow row : summary.runtimeRows) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "live.runtime");
            item.itemKey = safe(row.strategyName) + "@" + safe(row.strategyVersion);
            item.itemName = safe(row.strategyName);
            item.metricValue = row.realizedPnl == null ? 0D : row.realizedPnl;
            item.metricText = "signals=" + nz(row.signalCount)
                    + ", orders=" + nz(row.orderCount)
                    + ", trades=" + nz(row.tradeCount)
                    + ", errors=" + nz(row.errorCount);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
        for (BlockedRunRow row : summary.blockedRuns) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "pipeline.blocked");
            item.itemKey = safe(row.runId);
            item.itemName = safe(row.currentStage);
            item.metricValue = 1D;
            item.metricText = safe(row.currentStatus) + " / " + safe(row.currentReason);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
        return items;
    }

    private void addCountItems(List<ReportItem> items, String reportId, String reportDate, String section, List<CountRow> rows) {
        for (CountRow row : rows) {
            ReportItem item = initItem(reportId, reportDate, section);
            item.itemKey = safe(row.key1) + "|" + safe(row.key2);
            item.itemName = safe(row.key1);
            item.metricValue = row.total == null ? 0D : row.total.doubleValue();
            item.metricText = safe(row.key2);
            item.payload = JsonUtils.Serializer(row);
            items.add(item);
        }
    }

    private ReportItem initItem(String reportId, String reportDate, String section) {
        ReportItem item = new ReportItem();
        item.id = reportId + "_" + section + "_" + System.nanoTime();
        item.reportId = reportId;
        item.reportDate = reportDate;
        item.section = section;
        item.createTime = LocalDateTime.now().format(TS);
        return item;
    }

    private long sum(List<CountRow> rows) {
        long total = 0L;
        for (CountRow row : rows) {
            total += row.total == null ? 0L : row.total.longValue();
        }
        return total;
    }

    private long sumByStatus(List<CountRow> rows, String status) {
        long total = 0L;
        for (CountRow row : rows) {
            if (StringUtils.equalsIgnoreCase(status, row.key2)) {
                total += row.total == null ? 0L : row.total.longValue();
            }
        }
        return total;
    }

    private long sumRuntime(List<RuntimeStrategyRow> rows, String type) {
        long total = 0L;
        if (rows == null) {
            return total;
        }
        for (RuntimeStrategyRow row : rows) {
            if ("signal".equalsIgnoreCase(type)) {
                total += nz(row.signalCount);
            } else if ("order".equalsIgnoreCase(type)) {
                total += nz(row.orderCount);
            } else if ("trade".equalsIgnoreCase(type)) {
                total += nz(row.tradeCount);
            } else if ("error".equalsIgnoreCase(type)) {
                total += nz(row.errorCount);
            }
        }
        return total;
    }

    private double sumRuntimePnl(List<RuntimeStrategyRow> rows) {
        double total = 0D;
        if (rows == null) {
            return total;
        }
        for (RuntimeStrategyRow row : rows) {
            total += row.realizedPnl == null ? 0D : row.realizedPnl;
        }
        return total;
    }

    private List<CountRow> convertExternal(List<com.app.dc.service.externalsource.ExternalSourceModels.CountRow> rows) {
        List<CountRow> result = new ArrayList<CountRow>();
        if (rows == null) {
            return result;
        }
        for (com.app.dc.service.externalsource.ExternalSourceModels.CountRow row : rows) {
            CountRow target = new CountRow();
            target.key1 = row.key1;
            target.key2 = row.key2;
            target.total = row.total;
            result.add(target);
        }
        return result;
    }

    private String cssStatus(String status) {
        if (StringUtils.equalsAnyIgnoreCase(status, "SUCCESS", "ACTIVE")) {
            return "ok";
        }
        if (StringUtils.equalsAnyIgnoreCase(status, "SKIPPED", "SUSPENDED")) {
            return "warn";
        }
        return "bad";
    }

    private String scale(Double value) {
        if (value == null) {
            return "0";
        }
        return String.format(java.util.Locale.ENGLISH, "%.6f", value);
    }

    private long nz(Long value) {
        return value == null ? 0L : value.longValue();
    }

    private String escape(String text) {
        String value = text == null ? "" : text;
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private String safe(String text) {
        return text == null ? "" : text;
    }
}
