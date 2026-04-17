package com.app.dc.service.systemreport;

import com.app.common.utils.JsonUtils;
import com.app.dc.service.systemreport.StrategySystemReportModels.ActiveLiveRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.BacktestDetailRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.BlockedRunRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.GenerationDetailRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReportFiles;
import com.app.dc.service.systemreport.StrategySystemReportModels.RuntimeReportSummary;
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
public class StrategySystemRuntimeReportService {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter REPORT_ID_TS = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    @Autowired
    private ClickHouseStrategySystemReportDao systemReportDao;

    @Value("${strategy.system.runtime.report.enabled:true}")
    private boolean enabled;

    @Value("${strategy.system.runtime.report.reportDir:../../data/BatchSvr/strategy-system-runtime-reports}")
    private String reportDir;

    @Value("${strategy.system.runtime.report.detailLimit:50}")
    private int detailLimit;

    public String generateRuntimeReport() {
        if (!enabled || !systemReportDao.ready()) {
            return "";
        }
        LocalDateTime now = LocalDateTime.now();
        LocalDate reportDate = now.toLocalDate();
        String snapshotTime = now.format(TS);
        String reportId = "system_runtime_report";
        String since = now.minusDays(1).format(TS);

        RuntimeReportSummary summary = new RuntimeReportSummary();
        summary.reportId = reportId;
        summary.reportDate = reportDate.toString();
        summary.snapshotTime = snapshotTime;
        summary.activeLives.addAll(systemReportDao.loadCurrentActiveLives());
        summary.runtimeRows.addAll(systemReportDao.loadRuntimeStrategyRows(summary.reportDate));
        summary.runningGenerationTasks.addAll(filterRunningGeneration(systemReportDao.loadLatestGenerationDetailsSince(since, detailLimit * 4)));
        summary.runningBacktestTasks.addAll(filterRunningBacktest(systemReportDao.loadLatestBacktestDetailsSince(since, detailLimit * 4)));
        summary.blockedRuns.addAll(filterFailureRows(systemReportDao.loadBlockedRunsSince(since, detailLimit * 4)));

        summary.headline = new LinkedHashMap<String, Object>();
        summary.headline.put("activeLiveTotal", summary.activeLives.size());
        summary.headline.put("runtimeSignalTotal", sumRuntime(summary.runtimeRows, "signal"));
        summary.headline.put("runtimeOrderTotal", sumRuntime(summary.runtimeRows, "order"));
        summary.headline.put("runtimeTradeTotal", sumRuntime(summary.runtimeRows, "trade"));
        summary.headline.put("runtimePnlTotal", scale(sumRuntimePnl(summary.runtimeRows)));
        summary.headline.put("runtimeErrorTotal", sumRuntime(summary.runtimeRows, "error"));
        summary.headline.put("runningGenerationTotal", summary.runningGenerationTasks.size());
        summary.headline.put("runningBacktestTotal", summary.runningBacktestTasks.size());
        summary.headline.put("blockedFailureTotal", summary.blockedRuns.size());

        ReportFiles files = writeFiles(reportId, summary);
        return files.htmlPath;
    }

    private ReportFiles writeFiles(String reportId, RuntimeReportSummary summary) {
        ReportFiles files = new ReportFiles();
        try {
            Path dayDir = Paths.get(reportDir);
            Files.createDirectories(dayDir);
            files.jsonPath = dayDir.resolve(reportId + ".json").toString();
            files.htmlPath = dayDir.resolve(reportId + ".html").toString();
            Files.write(Paths.get(files.jsonPath), JsonUtils.Serializer(summary).getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(files.htmlPath), buildHtml(summary).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("StrategySystemRuntimeReportService writeFiles error, reportId:{}", reportId, e);
        }
        return files;
    }

    private String buildHtml(RuntimeReportSummary summary) {
        StringBuilder html = new StringBuilder();
        html.append("<html><head><meta charset=\"UTF-8\"><title>\u7cfb\u7edf\u8fd0\u884c\u62a5\u544a</title>")
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
        html.append("<h1>\u7cfb\u7edf\u8fd0\u884c\u62a5\u544a</h1>");
        html.append("<div class='muted'>\u62a5\u544a\u65e5\u671f: ").append(escape(summary.reportDate))
                .append(" / \u5feb\u7167\u65f6\u95f4: ").append(escape(summary.snapshotTime)).append("</div>");
        html.append("<div class='cards'>");
        appendCard(html, "\u5f53\u524d ACTIVE", summary.headline.get("activeLiveTotal"));
        appendCard(html, "\u4eca\u65e5\u4fe1\u53f7", summary.headline.get("runtimeSignalTotal"));
        appendCard(html, "\u4eca\u65e5\u8ba2\u5355", summary.headline.get("runtimeOrderTotal"));
        appendCard(html, "\u4eca\u65e5\u6210\u4ea4", summary.headline.get("runtimeTradeTotal"));
        appendCard(html, "\u4eca\u65e5\u5df2\u5b9e\u73b0 PnL", summary.headline.get("runtimePnlTotal"));
        appendCard(html, "\u4eca\u65e5\u5f02\u5e38", summary.headline.get("runtimeErrorTotal"));
        appendCard(html, "\u6b63\u5728\u751f\u6210", summary.headline.get("runningGenerationTotal"));
        appendCard(html, "\u6b63\u5728\u56de\u6d4b", summary.headline.get("runningBacktestTotal"));
        appendCard(html, "\u5f53\u524d\u5931\u8d25/\u5f02\u5e38", summary.headline.get("blockedFailureTotal"));
        html.append("</div>");

        appendActiveLives(html, summary.activeLives);
        appendRuntimeRows(html, summary.runtimeRows);
        appendGenerationTasks(html, summary.runningGenerationTasks);
        appendBacktestTasks(html, summary.runningBacktestTasks);
        appendBlockedRuns(html, "\u5f53\u524d\u5931\u8d25/\u5f02\u5e38\u7b56\u7565", summary.blockedRuns,
                "\u5f53\u524d\u6ca1\u6709 FAILED / SUSPENDED \u7684\u7b56\u7565\u6d41\u6c34\u7ebf\u5f02\u5e38");
        html.append("</body></html>");
        return html.toString();
    }

    private void appendCard(StringBuilder html, String title, Object value) {
        html.append("<div class='card'><div class='label'>").append(escape(title))
                .append("</div><div class='value'>").append(escape(String.valueOf(value))).append("</div></div>");
    }

    private void appendActiveLives(StringBuilder html, List<ActiveLiveRow> rows) {
        html.append("<h2>\u5f53\u524d ACTIVE \u6e05\u5355</h2><table><tr><th>\u7b56\u7565</th><th>\u7248\u672c</th><th>\u573a\u666f</th><th>\u8fd0\u884c\u7c7b\u578b</th><th>Symbol</th><th>\u5468\u671f</th><th>\u751f\u6548\u65f6\u95f4</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>\u5f53\u524d\u6ca1\u6709 ACTIVE \u7b56\u7565</td></tr>");
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
        html.append("<h2>\u4eca\u65e5\u5b9e\u76d8\u8fd0\u884c\u6458\u8981</h2><table><tr><th>\u7b56\u7565</th><th>\u7248\u672c</th><th>\u4fe1\u53f7\u6570</th><th>\u8ba2\u5355\u6570</th><th>\u6210\u4ea4\u6570</th><th>\u5df2\u5b9e\u73b0 PnL</th><th>\u5f02\u5e38\u6570</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>\u4eca\u65e5\u6682\u65e0\u5b9e\u76d8\u8fd0\u884c\u6570\u636e</td></tr>");
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

    private void appendGenerationTasks(StringBuilder html, List<GenerationDetailRow> rows) {
        html.append("<h2>\u5f53\u524d\u6b63\u5728\u751f\u6210\u7684\u7b56\u7565</h2><table><tr><th>\u6765\u6e90</th><th>\u7b56\u7565</th><th>\u7248\u672c</th><th>\u573a\u666f</th><th>\u751f\u6210\u72b6\u6001</th><th>\u7f16\u8bd1\u72b6\u6001</th><th>\u66f4\u65b0\u65f6\u95f4</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>\u5f53\u524d\u6ca1\u6709 RUNNING / PENDING / QUEUED \u7684 generation task</td></tr>");
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

    private void appendBacktestTasks(StringBuilder html, List<BacktestDetailRow> rows) {
        html.append("<h2>\u5f53\u524d\u6b63\u5728\u56de\u6d4b\u7684\u7b56\u7565</h2><table><tr><th>\u7b56\u7565</th><th>\u7248\u672c</th><th>\u57fa\u7ebf\u7248\u672c</th><th>\u4efb\u52a1\u7c7b\u578b</th><th>\u72b6\u6001</th><th>\u6302\u8d77\u539f\u56e0</th><th>\u66f4\u65b0\u65f6\u95f4</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='7'>\u5f53\u524d\u6ca1\u6709 RUNNING / PENDING \u7684 backtest task</td></tr>");
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

    private void appendBlockedRuns(StringBuilder html, String title, List<BlockedRunRow> rows, String emptyText) {
        html.append("<h2>").append(escape(title))
                .append("</h2><table><tr><th>runId</th><th>\u6765\u6e90</th><th>\u7b56\u7565</th><th>\u7248\u672c</th><th>\u5f53\u524d\u9636\u6bb5</th><th>\u72b6\u6001</th><th>\u539f\u56e0</th><th>\u66f4\u65b0\u65f6\u95f4</th></tr>");
        if (rows == null || rows.isEmpty()) {
            html.append("<tr><td colspan='8'>").append(escape(emptyText)).append("</td></tr>");
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

    private List<GenerationDetailRow> filterRunningGeneration(List<GenerationDetailRow> rows) {
        List<GenerationDetailRow> result = new ArrayList<GenerationDetailRow>();
        if (rows == null) {
            return result;
        }
        for (GenerationDetailRow row : rows) {
            if (row == null) {
                continue;
            }
            if (StringUtils.equalsAnyIgnoreCase(row.status, "RUNNING", "PENDING")
                    || StringUtils.equalsAnyIgnoreCase(row.compileStatus, "QUEUED")) {
                result.add(row);
                if (result.size() >= detailLimit) {
                    break;
                }
            }
        }
        return result;
    }

    private List<BacktestDetailRow> filterRunningBacktest(List<BacktestDetailRow> rows) {
        List<BacktestDetailRow> result = new ArrayList<BacktestDetailRow>();
        if (rows == null) {
            return result;
        }
        for (BacktestDetailRow row : rows) {
            if (row == null) {
                continue;
            }
            if (StringUtils.equalsAnyIgnoreCase(row.status, "RUNNING", "PENDING")) {
                result.add(row);
                if (result.size() >= detailLimit) {
                    break;
                }
            }
        }
        return result;
    }

    private List<BlockedRunRow> filterFailureRows(List<BlockedRunRow> rows) {
        List<BlockedRunRow> result = new ArrayList<BlockedRunRow>();
        if (rows == null) {
            return result;
        }
        for (BlockedRunRow row : rows) {
            if (row == null || isExpectedSkip(row)) {
                continue;
            }
            result.add(row);
            if (result.size() >= detailLimit) {
                break;
            }
        }
        return result;
    }

    private String cssStatus(String status) {
        if (StringUtils.equalsAnyIgnoreCase(status, "SUCCESS", "ACTIVE")) {
            return "ok";
        }
        if (StringUtils.equalsAnyIgnoreCase(status, "RUNNING", "PENDING", "QUEUED", "SKIPPED", "SUSPENDED")) {
            return "warn";
        }
        return "bad";
    }

    private boolean isExpectedSkip(BlockedRunRow row) {
        if (row == null || !StringUtils.equalsIgnoreCase(row.currentStatus, "SKIPPED")) {
            return false;
        }
        return StringUtils.equalsAnyIgnoreCase(row.currentReason,
                "no trades on trade date",
                "no signals on trade date",
                "no orders on trade date",
                "ALGO_UNCHANGED");
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
}
