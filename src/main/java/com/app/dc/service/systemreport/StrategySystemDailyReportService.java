package com.app.dc.service.systemreport;

import com.app.common.utils.JsonUtils;
import com.app.dc.service.externalsource.ClickHouseExternalSourceDao;
import com.app.dc.service.systemreport.StrategySystemReportModels.ActiveLiveRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.CountRow;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReportFiles;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReportItem;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReportSummary;
import com.app.dc.service.systemreport.StrategySystemReportModels.ReviewFactRow;
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
import java.util.Map;

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

    @Value("${strategy.system.report.reportDir:./data/strategy-system-reports}")
    private String reportDir;

    public String generateDailyReport() {
        if (!enabled || !systemReportDao.ready()) {
            return "";
        }
        LocalDate reportDate = LocalDate.now();
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
        summary.reviewFactCounts.addAll(systemReportDao.countReviewFactsBySeveritySince(reportDate.toString()));
        summary.activeLives.addAll(systemReportDao.loadCurrentActiveLives());
        summary.topReviewFacts.addAll(systemReportDao.loadTopReviewFacts(reportDate.toString(), 20));
        summary.headline.put("rawTotal", sum(summary.rawCounts));
        summary.headline.put("readyNormTotal", sumByStatus(summary.normCounts, "READY"));
        summary.headline.put("generatedTotal", sumByStatus(summary.generationCounts, "SUCCESS"));
        summary.headline.put("backtestSuccessTotal", sumByStatus(summary.backtestCounts, "SUCCESS"));
        summary.headline.put("optimizationTrialTotal", sum(summary.optimizationCounts));
        summary.headline.put("publishEventTotal", sum(summary.releaseCounts));
        summary.headline.put("activeLiveTotal", summary.activeLives.size());
        summary.headline.put("reviewFactTotal", sum(summary.reviewFactCounts));

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
        html.append("<html><head><meta charset=\"UTF-8\"><title>策略系统日报</title>")
                .append("<style>body{font-family:Segoe UI,Microsoft YaHei,sans-serif;margin:24px;color:#1f2937;}")
                .append("h1,h2{margin:0 0 12px 0;}table{border-collapse:collapse;width:100%;margin:12px 0 24px 0;}")
                .append("th,td{border:1px solid #d1d5db;padding:8px 10px;font-size:13px;text-align:left;}")
                .append("th{background:#f3f4f6;} .cards{display:flex;gap:12px;flex-wrap:wrap;margin:16px 0 24px 0;}")
                .append(".card{border:1px solid #d1d5db;border-radius:8px;padding:12px 16px;min-width:180px;background:#fff;}")
                .append(".label{font-size:12px;color:#6b7280;} .value{font-size:22px;font-weight:600;margin-top:4px;}</style>")
                .append("</head><body>");
        html.append("<h1>策略系统日报</h1>");
        html.append("<div>报告日期: ").append(summary.reportDate).append(" / 生成时间: ").append(summary.generatedAt).append("</div>");
        html.append("<div class='cards'>");
        appendCard(html, "采集 Raw", summary.headline.get("rawTotal"));
        appendCard(html, "待投递 Norm", summary.headline.get("readyNormTotal"));
        appendCard(html, "生成成功", summary.headline.get("generatedTotal"));
        appendCard(html, "回测成功", summary.headline.get("backtestSuccessTotal"));
        appendCard(html, "优化 Trial", summary.headline.get("optimizationTrialTotal"));
        appendCard(html, "当前 ACTIVE", summary.headline.get("activeLiveTotal"));
        html.append("</div>");
        appendCountTable(html, "采集阶段 Raw", summary.rawCounts);
        appendCountTable(html, "采集阶段 Norm", summary.normCounts);
        appendCountTable(html, "生成阶段", summary.generationCounts);
        appendCountTable(html, "回测阶段", summary.backtestCounts);
        appendCountTable(html, "优化阶段", summary.optimizationCounts);
        appendCountTable(html, "发布阶段", summary.releaseCounts);
        appendCountTable(html, "实盘复盘事实", summary.reviewFactCounts);
        appendActiveLives(html, summary.activeLives);
        appendReviewFacts(html, summary.topReviewFacts);
        html.append("</body></html>");
        return html.toString();
    }

    private void appendCard(StringBuilder html, String title, Object value) {
        html.append("<div class='card'><div class='label'>").append(escape(title))
                .append("</div><div class='value'>").append(escape(String.valueOf(value))).append("</div></div>");
    }

    private void appendCountTable(StringBuilder html, String title, List<CountRow> rows) {
        html.append("<h2>").append(escape(title)).append("</h2><table><tr><th>维度1</th><th>维度2</th><th>数量</th></tr>");
        for (CountRow row : rows) {
            html.append("<tr><td>").append(escape(row.key1)).append("</td><td>")
                    .append(escape(row.key2)).append("</td><td>")
                    .append(row.total == null ? 0L : row.total).append("</td></tr>");
        }
        html.append("</table>");
    }

    private void appendActiveLives(StringBuilder html, List<ActiveLiveRow> rows) {
        html.append("<h2>当前 ACTIVE 策略</h2><table><tr><th>策略</th><th>版本</th><th>场景</th><th>运行时</th><th>Symbol</th><th>周期</th><th>生效时间</th></tr>");
        for (ActiveLiveRow row : rows) {
            html.append("<tr><td>").append(escape(row.strategyName)).append("</td><td>")
                    .append(escape(row.strategyVersion)).append("</td><td>")
                    .append(escape(row.scene)).append("</td><td>")
                    .append(escape(row.runtimeType)).append("</td><td>")
                    .append(escape(row.symbolScope)).append("</td><td>")
                    .append(escape(row.textScope)).append("</td><td>")
                    .append(escape(row.effectiveTime)).append("</td></tr>");
        }
        html.append("</table>");
    }

    private void appendReviewFacts(StringBuilder html, List<ReviewFactRow> rows) {
        html.append("<h2>重点复盘事实</h2><table><tr><th>策略</th><th>版本</th><th>Symbol</th><th>周期</th><th>Fact</th><th>Severity</th><th>报告</th></tr>");
        for (ReviewFactRow row : rows) {
            html.append("<tr><td>").append(escape(row.strategyName)).append("</td><td>")
                    .append(escape(row.strategyVersion)).append("</td><td>")
                    .append(escape(row.symbol)).append("</td><td>")
                    .append(escape(row.text)).append("</td><td>")
                    .append(escape(row.factType)).append("</td><td>")
                    .append(row.severity == null ? 0 : row.severity).append("</td><td>")
                    .append(escape(row.reviewReportPath)).append("</td></tr>");
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
        addCountItems(items, summary.reportId, summary.reportDate, "review_fact", summary.reviewFactCounts);
        for (ActiveLiveRow row : summary.activeLives) {
            ReportItem item = initItem(summary.reportId, summary.reportDate, "live.active");
            item.itemKey = row.strategyName + "@" + row.strategyVersion;
            item.itemName = row.scene;
            item.metricValue = 1D;
            item.metricText = row.symbolScope + " / " + row.textScope;
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

    private String escape(String text) {
        String value = text == null ? "" : text;
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private String safe(String text) {
        return text == null ? "" : text;
    }
}
