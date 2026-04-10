package com.app.dc.service.externalsource;

import com.app.common.utils.JsonUtils;
import com.app.dc.pipeline.StrategyPipelineModels;
import com.app.dc.pipeline.StrategyPipelineService;
import com.app.dc.service.externalsource.ExternalSourceModels.CountRow;
import com.app.dc.service.externalsource.ExternalSourceModels.NormRecord;
import com.app.dc.service.externalsource.ExternalSourceModels.RawRecord;
import com.app.dc.service.externalsource.ExternalSourceModels.SourceType;
import com.app.dc.utils.INDSvrClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.HttpStatusException;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class ExternalSourceFacade {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern FMZ_SITEMAP_PATTERN = Pattern.compile("https://www\\.fmz\\.com/sitemap_\\d+\\.xml");
    private static final Pattern FMZ_STRATEGY_PATTERN = Pattern.compile("https://www\\.fmz\\.com/strategy/\\d+");
    private static final Pattern TRADINGVIEW_ENGLISH_TIME_PATTERN = Pattern.compile(
            "([A-Z][a-z]{2,8} \\d{1,2}, \\d{4}(?:, \\d{1,2}:\\d{2}(?::\\d{2})?(?: ?(?:AM|PM|UTC))?)?)");
    private static final Pattern GITHUB_HTML_REPO_PATH_PATTERN = Pattern.compile("^/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$");
    private static final Set<String> GITHUB_NON_REPO_ROOTS = new LinkedHashSet<String>(Arrays.asList(
            "about", "account", "apps", "blog", "collections", "contact", "copilot", "customer-stories",
            "enterprise", "events", "explore", "features", "gist", "gists", "github", "issues", "login",
            "logout", "marketplace", "new", "notifications", "orgs", "organizations", "pricing", "pulls",
            "readme", "search", "security", "settings", "signup", "site", "sponsors", "team", "topics", "users"
    ));

    @Autowired
    private ClickHouseExternalSourceDao externalSourceDao;

    @Autowired
    private ExternalSourceHttpSupport httpSupport;

    @Autowired(required = false)
    private StrategyPipelineService strategyPipelineService;

    @Autowired(required = false)
    private INDSvrClient indSvrClient;

    @Value("${external.source.enabled:true}")
    private boolean externalSourceEnabled;

    @Value("${external.source.dispatch.enabled:false}")
    private boolean dispatchEnabled;

    @Value("${external.normalize.batchSize:100}")
    private int normalizeBatchSize;

    @Value("${external.dispatch.batchSize:20}")
    private int dispatchBatchSize;

    @Value("${external.dispatch.provider:deepseek}")
    private String dispatchProvider;

    @Value("${external.tradingview.pageLimit:30}")
    private int tradingViewPageLimit;

    @Value("${external.fmz.pageLimit:20}")
    private int fmzPageLimit;

    @Value("${external.github.repoLimit:100}")
    private int githubRepoLimit;

    @Value("${external.github.minStars:5}")
    private int githubMinStars;

    @Value("${external.github.queryWindowDays:365}")
    private int githubQueryWindowDays;

    @Value("${external.github.readmeLimit:15}")
    private int githubReadmeLimit;

    @Value("${external.github.htmlQuerySleepMs:3000}")
    private long githubHtmlQuerySleepMs;

    @Value("${external.github.htmlRetryDelayMs:5000}")
    private long githubHtmlRetryDelayMs;

    @Value("${external.github.queries:crypto trading strategy|binance futures strategy|cryptocurrency trading bot|pinescript crypto strategy|tradingview strategy crypto}")
    private String githubQueries;

    @Value("${external.github.token:}")
    private String githubToken;

    @Value("${external.digest.reportDir:./data/external-source-reports}")
    private String digestReportDir;

    public boolean initDispatchClientIfNeeded() {
        if (!dispatchEnabled) {
            log.info("ExternalSourceFacade initDispatchClientIfNeeded skipped, dispatchEnabled:false");
            return false;
        }
        if (indSvrClient == null) {
            log.warn("ExternalSourceFacade initDispatchClientIfNeeded skipped, indSvrClient is null");
            return false;
        }
        try {
            indSvrClient.init();
            log.info("ExternalSourceFacade initDispatchClientIfNeeded done, provider:{}", dispatchProvider);
            return true;
        } catch (Exception e) {
            log.error("ExternalSourceFacade initDispatchClientIfNeeded error", e);
            return false;
        }
    }

    public int discoverTradingView() {
        if (!externalSourceEnabled || !externalSourceDao.ready()) {
            return 0;
        }
        int discovered = 0;
        Set<String> detailUrls = new LinkedHashSet<String>();
        for (int page = 1; page <= Math.max(1, tradingViewPageLimit); page++) {
            Document doc = loadFirstDocument(new String[]{
                    "https://www.tradingview.com/scripts/search/crypto/?script_type=strategies&page=" + page,
                    "https://www.tradingview.com/scripts/?script_type=strategies&page=" + page
            });
            if (doc == null) {
                continue;
            }
            detailUrls.addAll(extractLinks(doc.select("a[href]"), "https://www.tradingview.com", "/script/"));
        }
        for (String url : detailUrls) {
            discovered += captureTradingViewDetail(url);
        }
        log.info("ExternalSourceFacade discoverTradingView done, detailCount:{}, discovered:{}", detailUrls.size(), discovered);
        return discovered;
    }

    public int discoverFmz() {
        if (!externalSourceEnabled || !externalSourceDao.ready()) {
            return 0;
        }
        int discovered = 0;
        int targetCount = Math.max(1, fmzPageLimit) * 20;
        Set<String> detailUrls = loadFmzStrategyUrls(targetCount);
        if (detailUrls.isEmpty()) {
            log.warn("ExternalSourceFacade discoverFmz sitemap returned empty, fallback to legacy list pages");
            for (int page = 1; page <= Math.max(1, fmzPageLimit); page++) {
                Document doc = loadFirstDocument(new String[]{
                        "https://www.fmz.com/strategy?p=" + page,
                        "https://www.fmz.com/strategy?page=" + page,
                        "https://www.fmz.com/strategy"
                });
                if (doc == null) {
                    continue;
                }
                detailUrls.addAll(extractLinks(doc.select("a[href]"), "https://www.fmz.com", "/strategy/"));
                if (detailUrls.size() >= targetCount) {
                    break;
                }
            }
        }
        for (String url : detailUrls) {
            discovered += captureFmzDetailV2(url);
        }
        log.info("ExternalSourceFacade discoverFmz done, detailCount:{}, discovered:{}", detailUrls.size(), discovered);
        return discovered;
    }

    public int discoverGitHub() {
        if (!externalSourceEnabled || !externalSourceDao.ready()) {
            return 0;
        }
        int discovered = 0;
        int remaining = Math.max(1, githubRepoLimit);
        int enriched = 0;
        Set<String> seenRepos = new LinkedHashSet<String>();
        for (String query : resolveGithubQueries()) {
            if (remaining <= 0) {
                break;
            }
            int queryLimit = Math.min(remaining, 20);
            List<GitHubRepoSnapshot> snapshots = discoverGitHubByApi(query, queryLimit);
            boolean usedHtmlFallback = false;
            if (snapshots.isEmpty()) {
                snapshots = discoverGitHubByHtml(query, queryLimit);
                usedHtmlFallback = true;
            }
            for (GitHubRepoSnapshot snapshot : snapshots) {
                if (remaining <= 0) {
                    break;
                }
                if (!seenRepos.add(snapshot.fullName)) {
                    continue;
                }
                StringBuilder content = new StringBuilder();
                content.append(StringUtils.defaultString(snapshot.description));
                if (StringUtils.isNotBlank(snapshot.readme) && enriched < Math.max(1, githubReadmeLimit)) {
                    content.append("\nREADME: ").append(ExternalSourceUtils.truncate(snapshot.readme, 8000));
                    enriched++;
                }
                if (StringUtils.isNotBlank(githubToken)) {
                    for (String snippet : fetchGitHubFileSnippets(snapshot.fullName)) {
                        content.append("\nFILE: ").append(ExternalSourceUtils.truncate(snippet, 2500));
                    }
                }
                Map<String, Object> payload = new LinkedHashMap<String, Object>();
                payload.put("sourceType", SourceType.GITHUB.name());
                payload.put("siteName", SourceType.GITHUB.siteName());
                payload.put("fullName", snapshot.fullName);
                payload.put("htmlUrl", snapshot.htmlUrl);
                payload.put("defaultBranch", snapshot.defaultBranch);
                payload.put("stars", snapshot.stars);
                payload.put("forks", snapshot.forks);
                payload.put("language", snapshot.language);
                payload.put("topics", snapshot.topics);
                payload.put("query", query);
                payload.put("description", snapshot.description);
                payload.put("fallbackMode", snapshot.fallbackMode);
                discovered += persistRaw(SourceType.GITHUB, snapshot.fullName, snapshot.htmlUrl, snapshot.htmlUrl,
                        snapshot.fullName, snapshot.author, snapshot.publishedAt, snapshot.updatedAt, content.toString(), payload);
                remaining--;
            }
            if (usedHtmlFallback && StringUtils.isBlank(githubToken)) {
                sleepQuietly(githubHtmlQuerySleepMs);
            }
        }
        log.info("ExternalSourceFacade discoverGitHub done, discovered:{}", discovered);
        return discovered;
    }

    public int normalizeReady() {
        if (!externalSourceEnabled || !externalSourceDao.ready()) {
            return 0;
        }
        List<RawRecord> rows = externalSourceDao.pullLatestRawByStatus("NEW", Math.max(1, normalizeBatchSize));
        int processed = 0;
        for (RawRecord raw : rows) {
            Map<String, Object> rawPayload = parseJsonMap(raw == null ? null : raw.payload);
            String runId = ensurePipelineRunId(raw == null ? null : raw.sourceType,
                    raw == null ? null : raw.externalId,
                    null,
                    null,
                    rawPayload);
            try {
                NormRecord norm = buildNorm(raw);
                if (norm == null) {
                    externalSourceDao.markRawStatus(raw.id, "IGNORED", "{\"reason\":\"normalize_empty\"}");
                    markPipeline(raw == null ? null : raw.sourceType,
                            raw == null ? null : raw.externalId,
                            null,
                            null,
                            StrategyPipelineModels.NORMALIZE,
                            StrategyPipelineModels.SKIPPED,
                            "NORMALIZE_EMPTY",
                            null,
                            rawPayload,
                            runId);
                    continue;
                }
                boolean duplicateNorm = externalSourceDao.existsNormFingerprint(norm.scene, norm.strategyName, norm.fingerprint)
                        || externalSourceDao.existsNormByCanonicalUrl(norm.sourceType, norm.canonicalUrl);
                if (duplicateNorm) {
                    Map<String, Object> payload = new LinkedHashMap<String, Object>();
                    payload.put("reason", "duplicate_existing_norm");
                    payload.put("scene", norm.scene);
                    payload.put("strategyName", norm.strategyName);
                    payload.put("canonicalUrl", norm.canonicalUrl);
                    externalSourceDao.markRawStatus(raw.id, "IGNORED", JsonUtils.Serializer(payload));
                    markPipeline(raw == null ? null : raw.sourceType,
                            norm.id,
                            norm.strategyName,
                            null,
                            StrategyPipelineModels.NORMALIZE,
                            StrategyPipelineModels.SKIPPED,
                            "DUPLICATE_NORM",
                            null,
                            payload,
                            runId);
                    continue;
                }
                norm.status = "READY";
                boolean inserted = externalSourceDao.insertNormIfAbsent(norm);
                if (!inserted) {
                    Map<String, Object> payload = new LinkedHashMap<String, Object>();
                    payload.put("reason", "duplicate_existing_norm");
                    payload.put("scene", norm.scene);
                    payload.put("strategyName", norm.strategyName);
                    payload.put("canonicalUrl", norm.canonicalUrl);
                    externalSourceDao.markRawStatus(raw.id, "IGNORED", JsonUtils.Serializer(payload));
                    markPipeline(raw == null ? null : raw.sourceType,
                            norm.id,
                            norm.strategyName,
                            null,
                            StrategyPipelineModels.NORMALIZE,
                            StrategyPipelineModels.SKIPPED,
                            "DUPLICATE_NORM",
                            null,
                            payload,
                            runId);
                    continue;
                }
                Map<String, Object> payload = new LinkedHashMap<String, Object>();
                payload.put("normId", norm.id);
                payload.put("normStatus", norm.status);
                payload.put("scene", norm.scene);
                payload.put("strategyName", norm.strategyName);
                externalSourceDao.markRawStatus(raw.id, "NORMALIZED", JsonUtils.Serializer(payload));
                payload.put(StrategyPipelineService.PIPELINE_RUN_ID, runId);
                markPipeline(norm.sourceType,
                        norm.id,
                        norm.strategyName,
                        null,
                        StrategyPipelineModels.NORMALIZE,
                        StrategyPipelineModels.SUCCESS,
                        "",
                        null,
                        payload,
                        runId);
                processed++;
            } catch (Exception e) {
                log.error("ExternalSourceFacade normalize error, rawId:{}", raw.id, e);
                externalSourceDao.markRawStatus(raw.id, "ERROR", "{\"reason\":\"normalize_exception\"}");
                markPipeline(raw == null ? null : raw.sourceType,
                        raw == null ? null : raw.externalId,
                        null,
                        null,
                        StrategyPipelineModels.NORMALIZE,
                        StrategyPipelineModels.FAILED,
                        "NORMALIZE_EXCEPTION",
                        null,
                        rawPayload,
                        runId);
            }
        }
        log.info("ExternalSourceFacade normalizeReady done, input:{}, processed:{}", rows.size(), processed);
        return processed;
    }

    public int dispatchReady() {
        if (!dispatchEnabled || indSvrClient == null) {
            log.info("ExternalSourceFacade dispatchReady skipped, enabled:{}, indSvrClientReady:{}",
                    dispatchEnabled, indSvrClient != null);
            return 0;
        }
        List<NormRecord> rows = externalSourceDao.pullLatestNormByStatus("READY", Math.max(1, dispatchBatchSize));
        int dispatched = 0;
        for (NormRecord row : rows) {
            Map<String, Object> normPayload = parseJsonMap(row == null ? null : row.payload);
            String runId = ensurePipelineRunId(row == null ? null : row.sourceType,
                    row == null ? null : row.id,
                    row == null ? null : row.strategyName,
                    null,
                    normPayload);
            LocalDateTime stageStart = LocalDateTime.now();
            try {
                externalSourceDao.markNormStatus(row.id, "DISPATCHING", row.payload);
                markPipeline(row.sourceType,
                        row.id,
                        row.strategyName,
                        null,
                        StrategyPipelineModels.DISPATCH,
                        StrategyPipelineModels.RUNNING,
                        "",
                        stageStart,
                        normPayload,
                        runId);
                LinkedHashMap<String, Object> request = new LinkedHashMap<String, Object>();
                request.put("sourceType", "EXTERNAL_SOURCE");
                request.put("sourceRef", row.id);
                request.put("scene", row.scene);
                request.put("description", row.description);
                request.put("strategyName", row.strategyName);
                request.put("provider", dispatchProvider);
                normPayload.put(StrategyPipelineService.PIPELINE_RUN_ID, runId);
                request.put("payload", normPayload);
                Map<String, Object> rsp = indSvrClient.requestStrategyCandidateGenerate(request);
                int code = number(rsp.get("code"));
                String msg = text(rsp.get("msg"));
                if (code == 0) {
                    externalSourceDao.markNormStatus(row.id, "GENERATED", JsonUtils.Serializer(rsp));
                    Map<String, Object> payload = new LinkedHashMap<String, Object>(normPayload);
                    payload.put("dispatchResponse", rsp);
                    markPipeline(row.sourceType,
                            row.id,
                            row.strategyName,
                            text(rsp.get("strategyVersion")),
                            StrategyPipelineModels.DISPATCH,
                            StrategyPipelineModels.SUCCESS,
                            "",
                            stageStart,
                            payload,
                            runId);
                } else if (StringUtils.containsIgnoreCase(msg, "skip")) {
                    externalSourceDao.markNormStatus(row.id, "SKIPPED", JsonUtils.Serializer(rsp));
                    Map<String, Object> payload = new LinkedHashMap<String, Object>(normPayload);
                    payload.put("dispatchResponse", rsp);
                    markPipeline(row.sourceType,
                            row.id,
                            row.strategyName,
                            text(rsp.get("strategyVersion")),
                            StrategyPipelineModels.DISPATCH,
                            StrategyPipelineModels.SKIPPED,
                            defaultIfBlank(msg, "DISPATCH_SKIPPED"),
                            stageStart,
                            payload,
                            runId);
                } else {
                    externalSourceDao.markNormStatus(row.id, "ERROR", JsonUtils.Serializer(rsp));
                    Map<String, Object> payload = new LinkedHashMap<String, Object>(normPayload);
                    payload.put("dispatchResponse", rsp);
                    markPipeline(row.sourceType,
                            row.id,
                            row.strategyName,
                            text(rsp.get("strategyVersion")),
                            StrategyPipelineModels.DISPATCH,
                            StrategyPipelineModels.FAILED,
                            defaultIfBlank(msg, "DISPATCH_FAILED"),
                            stageStart,
                            payload,
                            runId);
                }
                dispatched++;
            } catch (Exception e) {
                log.error("ExternalSourceFacade dispatch error, normId:{}", row.id, e);
                externalSourceDao.markNormStatus(row.id, "ERROR", "{\"reason\":\"dispatch_exception\"}");
                markPipeline(row == null ? null : row.sourceType,
                        row == null ? null : row.id,
                        row == null ? null : row.strategyName,
                        null,
                        StrategyPipelineModels.DISPATCH,
                        StrategyPipelineModels.FAILED,
                        "DISPATCH_EXCEPTION",
                        stageStart,
                        normPayload,
                        runId);
            }
        }
        log.info("ExternalSourceFacade dispatchReady done, rows:{}, dispatched:{}", rows.size(), dispatched);
        return dispatched;
    }

    public String writeDigestReport() {
        if (!externalSourceDao.ready()) {
            return "";
        }
        LocalDateTime sinceTime = LocalDateTime.now().minusDays(1);
        String since = sinceTime.format(TS);
        List<CountRow> rawCounts = externalSourceDao.countLatestRawBySiteAndStatusSince(since);
        List<CountRow> normCounts = externalSourceDao.countLatestNormBySiteAndStatusSince(since);
        List<CountRow> sceneCounts = externalSourceDao.countLatestNormBySceneSince(since);
        List<NormRecord> readySamples = externalSourceDao.loadLatestNormSamples("READY", 20);

        Map<String, Long> rawStatusTotals = aggregateByStatus(rawCounts);
        Map<String, Long> normStatusTotals = aggregateByStatus(normCounts);
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("generatedAt", ExternalSourceUtils.nowText());
        summary.put("since", since);
        summary.put("rawTotal", sumTotals(rawCounts));
        summary.put("normTotal", sumTotals(normCounts));
        summary.put("rawStatusTotals", rawStatusTotals);
        summary.put("normStatusTotals", normStatusTotals);
        summary.put("readyTotal", normStatusTotals.getOrDefault("READY", 0L));
        summary.put("duplicateTotal", normStatusTotals.getOrDefault("DUPLICATE", 0L));
        summary.put("errorTotal", rawStatusTotals.getOrDefault("ERROR", 0L) + normStatusTotals.getOrDefault("ERROR", 0L));
        summary.put("rawCounts", rawCounts);
        summary.put("normCounts", normCounts);
        summary.put("sceneCounts", sceneCounts);
        summary.put("readySamples", readySamples);

        try {
            Path reportDir = Paths.get(digestReportDir);
            Files.createDirectories(reportDir);
            String day = LocalDate.now().toString();
            Path jsonPath = reportDir.resolve("external-source-digest-" + day + ".json");
            Path mdPath = reportDir.resolve("external-source-digest-" + day + ".md");
            Files.write(jsonPath, JsonUtils.Serializer(summary).getBytes(StandardCharsets.UTF_8));
            Files.write(mdPath, buildDigestMarkdownV2(summary, rawCounts, normCounts, sceneCounts, readySamples).getBytes(StandardCharsets.UTF_8));
            log.info("ExternalSourceFacade writeDigestReport done, json:{}, markdown:{}", jsonPath, mdPath);
            return mdPath.toString();
        } catch (Exception e) {
            log.error("ExternalSourceFacade writeDigestReport error", e);
            return "";
        }
    }

    private String buildDigestMarkdown(Map<String, Object> summary,
                                       List<CountRow> rawCounts,
                                       List<CountRow> normCounts,
                                       List<CountRow> sceneCounts,
                                       List<NormRecord> readySamples) {
        StringBuilder sb = new StringBuilder();
        sb.append("# 外部策略采集日报\n\n");
        sb.append("- 生成时间: ").append(summary.get("generatedAt")).append("\n");
        sb.append("- 统计起点: ").append(summary.get("since")).append("\n");
        sb.append("- Raw 总数: ").append(summary.get("rawTotal")).append("\n");
        sb.append("- Norm 总数: ").append(summary.get("normTotal")).append("\n");
        sb.append("- READY 数量: ").append(summary.get("readyTotal")).append("\n");
        sb.append("- DUPLICATE 数量: ").append(summary.get("duplicateTotal")).append("\n");
        sb.append("- ERROR 数量: ").append(summary.get("errorTotal")).append("\n\n");

        sb.append("## Raw 统计\n\n");
        for (CountRow row : rawCounts) {
            sb.append("- ").append(row.key1).append(" / ").append(row.key2).append(": ").append(row.total).append("\n");
        }

        sb.append("\n## Norm 统计\n\n");
        for (CountRow row : normCounts) {
            sb.append("- ").append(row.key1).append(" / ").append(row.key2).append(": ").append(row.total).append("\n");
        }

        sb.append("\n## Scene 分布\n\n");
        for (CountRow row : sceneCounts) {
            sb.append("- ").append(row.key1).append(": ").append(row.total).append("\n");
        }

        sb.append("\n## READY 样本\n\n");
        if (readySamples == null || readySamples.isEmpty()) {
            sb.append("- 暂无 READY 样本\n");
        } else {
            for (NormRecord row : readySamples) {
                sb.append("- ").append(row.siteName).append(" | ").append(row.strategyName).append(" | ")
                        .append(row.scene).append(" | ").append(row.normalizedTitle).append("\n");
            }
        }
        return sb.toString();
    }

    private String buildDigestMarkdownV2(Map<String, Object> summary,
                                         List<CountRow> rawCounts,
                                         List<CountRow> normCounts,
                                         List<CountRow> sceneCounts,
                                         List<NormRecord> readySamples) {
        StringBuilder sb = new StringBuilder();
        sb.append("# 外部策略采集日报\n\n");
        sb.append("- 生成时间: ").append(summary.get("generatedAt")).append("\n");
        sb.append("- 统计起点: ").append(summary.get("since")).append("\n");
        sb.append("- Raw 总数: ").append(summary.get("rawTotal")).append("\n");
        sb.append("- Norm 总数: ").append(summary.get("normTotal")).append("\n");
        sb.append("- READY 数量: ").append(summary.get("readyTotal")).append("\n");
        sb.append("- DUPLICATE 数量: ").append(summary.get("duplicateTotal")).append("\n");
        sb.append("- ERROR 数量: ").append(summary.get("errorTotal")).append("\n\n");

        sb.append("## Raw 统计\n\n");
        for (CountRow row : rawCounts) {
            sb.append("- ").append(row.key1).append(" / ").append(row.key2).append(": ").append(row.total).append("\n");
        }

        sb.append("\n## Norm 统计\n\n");
        for (CountRow row : normCounts) {
            sb.append("- ").append(row.key1).append(" / ").append(row.key2).append(": ").append(row.total).append("\n");
        }

        sb.append("\n## Scene 分布\n\n");
        for (CountRow row : sceneCounts) {
            sb.append("- ").append(row.key1).append(": ").append(row.total).append("\n");
        }

        sb.append("\n## READY 样本\n\n");
        if (readySamples == null || readySamples.isEmpty()) {
            sb.append("- 暂无 READY 样本\n");
        } else {
            for (NormRecord row : readySamples) {
                sb.append("- ").append(row.siteName).append(" | ").append(row.strategyName).append(" | ")
                        .append(row.scene).append(" | ").append(row.normalizedTitle).append("\n");
            }
        }
        return sb.toString();
    }

    private NormRecord buildNorm(RawRecord raw) {
        if (raw == null) {
            return null;
        }
        String normalizedTitle = ExternalSourceUtils.safe(raw.title);
        String content = ExternalSourceUtils.truncate(raw.content, 12000);
        if (StringUtils.isBlank(normalizedTitle) || StringUtils.isBlank(content)) {
            return null;
        }
        String scene = ExternalSourceUtils.detectScene(normalizedTitle, content);
        String strategyName = ExternalSourceUtils.slug(normalizedTitle, ExternalSourceUtils.slug(raw.siteName, "strategy"));
        String description = ExternalSourceUtils.buildChineseDescription(raw.siteName, normalizedTitle, scene, content);
        String fingerprint = ExternalSourceUtils.sha256(scene + "|" + strategyName + "|" + content);
        NormRecord norm = new NormRecord();
        norm.id = "norm_" + strategyName + "_" + System.currentTimeMillis();
        norm.rawId = raw.id;
        norm.sourceType = raw.sourceType;
        norm.siteName = raw.siteName;
        norm.canonicalUrl = raw.canonicalUrl;
        norm.normalizedTitle = normalizedTitle;
        norm.scene = scene;
        norm.strategyName = strategyName;
        norm.description = description;
        norm.content = content;
        norm.rawContentHash = raw.contentHash;
        norm.fingerprint = fingerprint;
        norm.createTime = ExternalSourceUtils.nowText();
        norm.updateTime = norm.createTime;
        Map<String, Object> payload = parseJsonMap(raw.payload);
        payload.put("rawId", raw.id);
        payload.put("sourceType", raw.sourceType);
        payload.put("siteName", raw.siteName);
        payload.put("canonicalUrl", raw.canonicalUrl);
        payload.put("title", raw.title);
        payload.put("author", raw.author);
        payload.put("publishedTime", raw.publishedTime);
        payload.put("externalUpdateTime", raw.externalUpdateTime);
        payload.put("contentHash", raw.contentHash);
        norm.payload = JsonUtils.Serializer(payload);
        return norm;
    }

    private int captureTradingViewDetail(String url) {
        try {
            Document doc = httpSupport.getDocument(url);
            String rawHtml = doc.outerHtml();
            String title = firstNonBlank(
                    attr(doc, "meta[property=og:title]", "content"),
                    attr(doc, "meta[name=twitter:title]", "content"),
                    doc.title());
            String content = firstNonBlank(
                    attr(doc, "meta[property=og:description]", "content"),
                    attr(doc, "meta[name=description]", "content"),
                    text(doc.selectFirst("main")),
                    text(doc.body()));
            String author = firstNonBlank(
                    text(doc.selectFirst("a[href^=/u/]")),
                    extractFirstRegex(rawHtml, "\"username\":\"([^\"]+)\""));
            String publishedTime = firstNonBlank(
                    extractFirstRegex(rawHtml, "\"datePublished\":\"([^\"]+)\""),
                    attr(doc, "time[datetime]", "datetime"),
                    extractFirstRegex(rawHtml, TRADINGVIEW_ENGLISH_TIME_PATTERN));
            String updateTime = firstNonBlank(
                    extractFirstRegex(rawHtml, "\"dateModified\":\"([^\"]+)\""),
                    extractFirstRegex(rawHtml, "Updated on (" + TRADINGVIEW_ENGLISH_TIME_PATTERN.pattern() + ")"),
                    publishedTime);
            String canonicalUrl = firstNonBlank(attr(doc, "link[rel=canonical]", "href"), url);
            if (StringUtils.isBlank(title) || StringUtils.isBlank(content)) {
                return 0;
            }
            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("sourceType", SourceType.TRADINGVIEW.name());
            payload.put("siteName", SourceType.TRADINGVIEW.siteName());
            payload.put("canonicalUrl", canonicalUrl);
            payload.put("title", title);
            payload.put("author", author);
            payload.put("publishedTime", normalizeIsoDateTime(publishedTime));
            payload.put("updateTime", normalizeIsoDateTime(updateTime));
            return persistRaw(SourceType.TRADINGVIEW, extractExternalId(canonicalUrl, "/script/"), canonicalUrl, url, title,
                    author, normalizeIsoDateTime(publishedTime), normalizeIsoDateTime(updateTime), content, payload);
        } catch (Exception e) {
            log.warn("captureTradingViewDetail error, url:{}", url, e);
            return 0;
        }
    }

    private int captureFmzDetail(String url) {
        try {
            Document doc = httpSupport.getDocument(url);
            String jsonLd = selectJsonLd(doc);
            String title = firstNonBlank(
                    extractJsonLdField(jsonLd, "headline"),
                    attr(doc, "meta[property=og:title]", "content"),
                    text(doc.selectFirst("h1")),
                    doc.title());
            String content = firstNonBlank(
                    extractJsonLdField(jsonLd, "text"),
                    attr(doc, "meta[property=og:description]", "content"),
                    text(doc.selectFirst("main")),
                    text(doc.body()));
            String author = firstNonBlank(
                    extractJsonLdAuthorName(jsonLd),
                    text(doc.selectFirst("a[href*=/user/]")),
                    text(doc.selectFirst(".author")));
            String publishedTime = firstNonBlank(
                    extractJsonLdField(jsonLd, "datePublished"),
                    extractFirstRegex(doc.text(), "创建日期[:：]\\s*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"),
                    attr(doc, "time[datetime]", "datetime"));
            String updateTime = firstNonBlank(
                    extractJsonLdField(jsonLd, "dateModified"),
                    extractFirstRegex(doc.text(), "最后修改[:：]\\s*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"),
                    publishedTime);
            String canonicalUrl = firstNonBlank(
                    extractJsonLdField(jsonLd, "url"),
                    attr(doc, "link[rel=canonical]", "href"),
                    url);
            if (StringUtils.isBlank(title) || StringUtils.isBlank(content)) {
                return 0;
            }
            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("sourceType", SourceType.FMZ.name());
            payload.put("siteName", SourceType.FMZ.siteName());
            payload.put("canonicalUrl", canonicalUrl);
            payload.put("title", title);
            payload.put("author", author);
            payload.put("publishedTime", normalizeIsoDateTime(publishedTime));
            payload.put("updateTime", normalizeIsoDateTime(updateTime));
            return persistRaw(SourceType.FMZ, extractExternalId(canonicalUrl, "/strategy/"), canonicalUrl, url, title,
                    author, normalizeIsoDateTime(publishedTime), normalizeIsoDateTime(updateTime), content, payload);
        } catch (Exception e) {
            log.warn("captureFmzDetail error, url:{}", url, e);
            return 0;
        }
    }

    private int captureFmzDetailV2(String url) {
        try {
            Document doc = httpSupport.getDocument(url);
            String jsonLd = selectJsonLd(doc);
            String title = firstNonBlank(
                    extractJsonLdField(jsonLd, "headline"),
                    attr(doc, "meta[property=og:title]", "content"),
                    text(doc.selectFirst("h1")),
                    doc.title());
            String content = firstNonBlank(
                    extractJsonLdField(jsonLd, "text"),
                    attr(doc, "meta[property=og:description]", "content"),
                    text(doc.selectFirst("main")),
                    text(doc.body()));
            String author = firstNonBlank(
                    extractJsonLdAuthorName(jsonLd),
                    text(doc.selectFirst("a[href*=/user/]")),
                    text(doc.selectFirst(".author")));
            String publishedTime = firstNonBlank(
                    extractJsonLdField(jsonLd, "datePublished"),
                    extractFirstRegex(doc.text(), "创建日期[:：]\\s*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"),
                    attr(doc, "time[datetime]", "datetime"));
            String updateTime = firstNonBlank(
                    extractJsonLdField(jsonLd, "dateModified"),
                    extractFirstRegex(doc.text(), "最后修改[:：]\\s*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"),
                    publishedTime);
            String canonicalUrl = preferDetailCanonicalUrl(
                    extractJsonLdField(jsonLd, "url"),
                    attr(doc, "link[rel=canonical]", "href"),
                    url,
                    "/strategy/");
            if (StringUtils.isBlank(title) || StringUtils.isBlank(content)) {
                return 0;
            }
            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("sourceType", SourceType.FMZ.name());
            payload.put("siteName", SourceType.FMZ.siteName());
            payload.put("canonicalUrl", canonicalUrl);
            payload.put("title", title);
            payload.put("author", author);
            payload.put("publishedTime", normalizeIsoDateTime(publishedTime));
            payload.put("updateTime", normalizeIsoDateTime(updateTime));
            return persistRaw(SourceType.FMZ, extractExternalId(canonicalUrl, "/strategy/"), canonicalUrl, url, title,
                    author, normalizeIsoDateTime(publishedTime), normalizeIsoDateTime(updateTime), content, payload);
        } catch (Exception e) {
            log.warn("captureFmzDetailV2 error, url:{}", url, e);
            return 0;
        }
    }

    private String preferDetailCanonicalUrl(String primary, String secondary, String fallback, String token) {
        if (StringUtils.contains(primary, token)) {
            return ExternalSourceUtils.safe(primary);
        }
        if (StringUtils.contains(secondary, token)) {
            return ExternalSourceUtils.safe(secondary);
        }
        return ExternalSourceUtils.safe(fallback);
    }

    private int persistRaw(SourceType sourceType,
                           String externalId,
                           String canonicalUrl,
                           String sourceUrl,
                           String title,
                           String author,
                           String publishedTime,
                           String updateTime,
                           String content,
                           Map<String, Object> payload) {
        String finalExternalId = StringUtils.defaultIfBlank(externalId, ExternalSourceUtils.sha256(canonicalUrl));
        String contentHash = ExternalSourceUtils.sha256(title + "\n" + content);
        Map<String, Object> pipelinePayload = payload == null
                ? new LinkedHashMap<String, Object>()
                : new LinkedHashMap<String, Object>(payload);
        String runId = ensurePipelineRunId(sourceType == null ? null : sourceType.name(),
                finalExternalId,
                null,
                null,
                pipelinePayload);
        if (externalSourceDao.existsRawByExternalId(sourceType.name(), finalExternalId)
                || externalSourceDao.existsRawByCanonicalUrl(sourceType.name(), canonicalUrl)
                || externalSourceDao.existsSameRawContent(sourceType.name(), finalExternalId, contentHash)) {
            log.info("persistRaw skip duplicate, sourceType:{}, externalId:{}, canonicalUrl:{}", sourceType.name(), finalExternalId, canonicalUrl);
            markPipeline(sourceType == null ? null : sourceType.name(),
                    finalExternalId,
                    null,
                    null,
                    StrategyPipelineModels.DISCOVER,
                    StrategyPipelineModels.SKIPPED,
                    "DUPLICATE_RAW",
                    null,
                    pipelinePayload,
                    runId);
            return 0;
        }
        RawRecord row = new RawRecord();
        row.id = "raw_" + ExternalSourceUtils.slug(sourceType.name().toLowerCase(Locale.ROOT), "src")
                + "_" + System.currentTimeMillis()
                + "_" + contentHash.substring(0, Math.min(12, contentHash.length()));
        row.sourceType = sourceType.name();
        row.siteName = sourceType.siteName();
        row.sourceName = sourceType.siteName();
        row.externalId = finalExternalId;
        row.canonicalUrl = canonicalUrl;
        row.sourceUrl = sourceUrl;
        row.title = ExternalSourceUtils.truncate(title, 512);
        row.author = ExternalSourceUtils.truncate(author, 256);
        row.publishedTime = publishedTime;
        row.externalUpdateTime = updateTime;
        row.contentHash = contentHash;
        row.content = ExternalSourceUtils.truncate(content, 16000);
        row.status = "NEW";
        row.createTime = ExternalSourceUtils.nowText();
        pipelinePayload.put(StrategyPipelineService.PIPELINE_RUN_ID, runId);
        row.payload = JsonUtils.Serializer(pipelinePayload);
        boolean inserted = externalSourceDao.insertRawIfAbsent(row);
        if (!inserted) {
            log.info("persistRaw skip duplicate by insert guard, sourceType:{}, externalId:{}, canonicalUrl:{}",
                    sourceType.name(), finalExternalId, canonicalUrl);
            markPipeline(sourceType == null ? null : sourceType.name(),
                    finalExternalId,
                    null,
                    null,
                    StrategyPipelineModels.DISCOVER,
                    StrategyPipelineModels.SKIPPED,
                    "DUPLICATE_RAW",
                    null,
                    pipelinePayload,
                    runId);
            return 0;
        }
        Map<String, Object> discoverPayload = new LinkedHashMap<String, Object>(pipelinePayload);
        discoverPayload.put("rawId", row.id);
        discoverPayload.put("canonicalUrl", canonicalUrl);
        markPipeline(sourceType == null ? null : sourceType.name(),
                finalExternalId,
                null,
                null,
                StrategyPipelineModels.DISCOVER,
                StrategyPipelineModels.SUCCESS,
                "",
                null,
                discoverPayload,
                runId);
        return 1;
    }

    private Map<String, Object> parseJsonMap(String json) {
        if (strategyPipelineService == null) {
            return new LinkedHashMap<String, Object>();
        }
        return strategyPipelineService.parsePayload(json);
    }

    private String ensurePipelineRunId(String sourceType,
                                       String sourceRef,
                                       String strategyName,
                                       String strategyVersion,
                                       Map<String, Object> payload) {
        if (strategyPipelineService == null) {
            return "";
        }
        return strategyPipelineService.ensureRunId(sourceType, sourceRef, strategyName, strategyVersion, payload);
    }

    private void markPipeline(String sourceType,
                              String sourceRef,
                              String strategyName,
                              String strategyVersion,
                              String stage,
                              String status,
                              String reason,
                              LocalDateTime stageStart,
                              Map<String, Object> payload,
                              String runId) {
        if (strategyPipelineService == null) {
            return;
        }
        Map<String, Object> effectivePayload = payload == null
                ? new LinkedHashMap<String, Object>()
                : new LinkedHashMap<String, Object>(payload);
        if (StringUtils.isNotBlank(runId)) {
            effectivePayload.put(StrategyPipelineService.PIPELINE_RUN_ID, runId);
        }
        strategyPipelineService.markStage(
                sourceType,
                sourceRef,
                strategyName,
                strategyVersion,
                stage,
                status,
                reason,
                stageStart,
                effectivePayload);
    }

    private String fetchGitHubReadme(String fullName) {
        if (StringUtils.isNotBlank(githubToken)) {
            try {
                String url = "https://api.github.com/repos/" + fullName + "/readme";
                String text = httpSupport.getText(url, githubHeaders("application/vnd.github.raw+json"));
                if (StringUtils.isNotBlank(text)) {
                    return text;
                }
            } catch (Exception e) {
                log.debug("fetchGitHubReadme api fallback, fullName:{}", fullName, e);
            }
        }
        return fetchGitHubReadmeFromHtml(fullName, null);
    }

    private String fetchGitHubReadmeFromHtml(String fullName, Document repoDoc) {
        try {
            Document doc = repoDoc == null ? httpSupport.getDocument("https://github.com/" + fullName) : repoDoc;
            return firstNonBlank(
                    text(doc.selectFirst("article.markdown-body")),
                    text(doc.selectFirst("#readme")),
                    text(doc.selectFirst("[data-testid=readme]")));
        } catch (Exception e) {
            log.debug("fetchGitHubReadmeFromHtml skip, fullName:{}", fullName, e);
            return "";
        }
    }

    private List<String> fetchGitHubFileSnippets(String fullName) {
        if (StringUtils.isBlank(githubToken)) {
            return Collections.emptyList();
        }
        try {
            String url = "https://api.github.com/repos/" + fullName + "/contents";
            List<Map<String, Object>> items = httpSupport.getJsonList(url, githubHeaders("application/vnd.github+json"));
            List<String> snippets = new ArrayList<String>();
            for (Map<String, Object> item : items) {
                if (snippets.size() >= 3) {
                    break;
                }
                String type = text(item.get("type"));
                String name = text(item.get("name")).toLowerCase(Locale.ROOT);
                String downloadUrl = text(item.get("download_url"));
                if (!"file".equals(type) || StringUtils.isBlank(downloadUrl)) {
                    continue;
                }
                if (!(name.endsWith(".py") || name.endsWith(".pine") || name.endsWith(".txt") || name.endsWith(".md") || name.contains("strategy"))) {
                    continue;
                }
                String body = httpSupport.getText(downloadUrl, Collections.<String, String>emptyMap());
                if (StringUtils.isNotBlank(body)) {
                    snippets.add(name + "\n" + ExternalSourceUtils.truncate(body, 2000));
                }
            }
            return snippets;
        } catch (Exception e) {
            log.debug("fetchGitHubFileSnippets skip, fullName:{}", fullName, e);
            return Collections.emptyList();
        }
    }

    private List<GitHubRepoSnapshot> discoverGitHubByApi(String query, int limit) {
        if (StringUtils.isBlank(githubToken)) {
            return Collections.emptyList();
        }
        String q = query
                + " archived:false fork:false stars:>=" + Math.max(0, githubMinStars)
                + " pushed:>=" + LocalDate.now().minusDays(Math.max(1, githubQueryWindowDays));
        String url = "https://api.github.com/search/repositories?q="
                + encodeUtf8(q)
                + "&sort=updated&order=desc&per_page=" + Math.max(1, limit) + "&page=1";
        try {
            Map<String, Object> response = httpSupport.getJsonObject(url, githubHeaders("application/vnd.github+json"));
            Object itemsObj = response.get("items");
            if (!(itemsObj instanceof List)) {
                return Collections.emptyList();
            }
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> items = (List<Map<String, Object>>) itemsObj;
            List<GitHubRepoSnapshot> snapshots = new ArrayList<GitHubRepoSnapshot>();
            for (Map<String, Object> item : items) {
                GitHubRepoSnapshot snapshot = new GitHubRepoSnapshot();
                snapshot.fullName = text(item.get("full_name"));
                snapshot.htmlUrl = text(item.get("html_url"));
                snapshot.author = text(item.get("owner") instanceof Map ? ((Map<?, ?>) item.get("owner")).get("login") : "");
                snapshot.description = text(item.get("description"));
                snapshot.updatedAt = normalizeIsoDateTime(text(item.get("updated_at")));
                snapshot.publishedAt = normalizeIsoDateTime(text(item.get("created_at")));
                snapshot.defaultBranch = text(item.get("default_branch"));
                snapshot.stars = text(item.get("stargazers_count"));
                snapshot.forks = text(item.get("forks_count"));
                snapshot.language = text(item.get("language"));
                snapshot.topics = item.get("topics");
                snapshot.readme = fetchGitHubReadme(snapshot.fullName);
                snapshot.fallbackMode = "api";
                if (StringUtils.isNotBlank(snapshot.fullName) && StringUtils.isNotBlank(snapshot.htmlUrl)) {
                    snapshots.add(snapshot);
                }
            }
            return snapshots;
        } catch (Exception e) {
            log.warn("discoverGitHubByApi error, query:{}", query, e);
            return Collections.emptyList();
        }
    }

    private List<GitHubRepoSnapshot> discoverGitHubByHtml(String query, int limit) {
        try {
            String url = "https://github.com/search?q=" + encodeUtf8(query) + "&type=repositories";
            Document doc = httpSupport.getDocument(url);
            Set<String> repoNames = new LinkedHashSet<String>();
            for (Element link : doc.select("a[href]")) {
                String href = ExternalSourceUtils.safe(link.attr("href"));
                if (!isGitHubRepoPath(href)) {
                    continue;
                }
                repoNames.add(href.substring(1));
                if (repoNames.size() >= Math.max(1, limit)) {
                    break;
                }
            }
            List<GitHubRepoSnapshot> result = new ArrayList<GitHubRepoSnapshot>();
            for (String fullName : repoNames) {
                GitHubRepoSnapshot snapshot = fetchGitHubRepoSnapshotFromHtml(fullName);
                if (snapshot != null) {
                    result.add(snapshot);
                }
            }
            log.info("discoverGitHubByHtml fallback used, query:{}, repoCount:{}", query, result.size());
            return result;
        } catch (Exception e) {
            log.warn("discoverGitHubByHtml error, query:{}", query, e);
            return Collections.emptyList();
        }
    }

    private GitHubRepoSnapshot fetchGitHubRepoSnapshotFromHtml(String fullName) {
        try {
            String htmlUrl = "https://github.com/" + fullName;
            Document doc = httpSupport.getDocument(htmlUrl);
            GitHubRepoSnapshot snapshot = new GitHubRepoSnapshot();
            snapshot.fullName = fullName;
            snapshot.htmlUrl = htmlUrl;
            snapshot.author = fullName.contains("/") ? fullName.substring(0, fullName.indexOf('/')) : "";
            snapshot.description = firstNonBlank(
                    attr(doc, "meta[property=og:description]", "content"),
                    attr(doc, "meta[name=description]", "content"),
                    text(doc.selectFirst("p.f4")),
                    text(doc.selectFirst("p[data-pjax=\"#repo-content-pjax-container\"]")));
            snapshot.updatedAt = normalizeIsoDateTime(firstNonBlank(
                    attr(doc, "relative-time[datetime]", "datetime"),
                    extractFirstRegex(doc.outerHtml(), "datetime=\"([0-9]{4}-[0-9]{2}-[0-9]{2}T[^\\\"]+)\"")));
            snapshot.publishedAt = "";
            snapshot.defaultBranch = "";
            snapshot.stars = "";
            snapshot.forks = "";
            snapshot.language = "";
            snapshot.topics = Collections.emptyList();
            snapshot.readme = fetchGitHubReadmeFromHtml(fullName, doc);
            snapshot.fallbackMode = "html";
            return snapshot;
        } catch (Exception e) {
            log.warn("fetchGitHubRepoSnapshotFromHtml error, fullName:{}", fullName, e);
            return null;
        }
    }

    private Map<String, String> githubHeaders(String accept) {
        Map<String, String> headers = new LinkedHashMap<String, String>();
        headers.put("Accept", accept);
        if (StringUtils.isNotBlank(githubToken)) {
            headers.put("Authorization", "Bearer " + githubToken.trim());
        }
        return headers;
    }

    private List<String> resolveGithubQueries() {
        List<String> result = new ArrayList<String>();
        for (String token : StringUtils.defaultString(githubQueries).split("\\|")) {
            if (StringUtils.isNotBlank(token)) {
                result.add(token.trim());
            }
        }
        if (result.isEmpty()) {
            result.addAll(Arrays.asList(
                    "crypto trading strategy",
                    "binance futures strategy",
                    "cryptocurrency trading bot"
            ));
        }
        return result;
    }

    private Set<String> extractLinks(Elements links, String baseUrl, String pathToken) {
        Set<String> result = new LinkedHashSet<String>();
        for (Element link : links) {
            String href = ExternalSourceUtils.safe(link.attr("href"));
            if (!href.contains(pathToken)) {
                continue;
            }
            String absolute = href.startsWith("http") ? href : baseUrl + href;
            int hashIndex = absolute.indexOf('#');
            if (hashIndex > 0) {
                absolute = absolute.substring(0, hashIndex);
            }
            int queryIndex = absolute.indexOf('?');
            if (queryIndex > 0) {
                absolute = absolute.substring(0, queryIndex);
            }
            result.add(absolute);
        }
        return result;
    }

    private Set<String> loadFmzStrategyUrls(int targetCount) {
        Set<String> detailUrls = new LinkedHashSet<String>();
        try {
            String sitemapIndex = httpSupport.getText("https://www.fmz.com/sitemap.xml", Collections.<String, String>emptyMap());
            List<String> sitemapUrls = collectMatches(sitemapIndex, FMZ_SITEMAP_PATTERN);
            for (String sitemapUrl : sitemapUrls) {
                if (detailUrls.size() >= targetCount) {
                    break;
                }
                String xml = httpSupport.getText(sitemapUrl, Collections.<String, String>emptyMap());
                for (String strategyUrl : collectMatches(xml, FMZ_STRATEGY_PATTERN)) {
                    detailUrls.add(strategyUrl);
                    if (detailUrls.size() >= targetCount) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("loadFmzStrategyUrls error", e);
        }
        return detailUrls;
    }

    private Document loadFirstDocument(String[] urls) {
        for (String url : urls) {
            try {
                Document doc = httpSupport.getDocument(url);
                if (doc != null && StringUtils.isNotBlank(doc.title())) {
                    return doc;
                }
            } catch (Exception e) {
                log.debug("loadFirstDocument skip, url:{}", url, e);
            }
        }
        return null;
    }

    private String extractExternalId(String url, String token) {
        int idx = url.indexOf(token);
        if (idx < 0) {
            return ExternalSourceUtils.sha256(url);
        }
        String remain = url.substring(idx + token.length());
        int slash = remain.indexOf('/');
        if (slash > 0) {
            remain = remain.substring(0, slash);
        }
        return StringUtils.defaultIfBlank(remain, ExternalSourceUtils.sha256(url));
    }

    private String normalizeIsoDateTime(String text) {
        String value = ExternalSourceUtils.safe(text);
        if (StringUtils.isBlank(value)) {
            return "";
        }
        value = value.replace("Published on ", "")
                .replace("Updated on ", "")
                .replace("Published ", "")
                .replace("Updated ", "")
                .trim();
        List<DateTimeFormatter> zoned = Arrays.asList(
                DateTimeFormatter.ISO_OFFSET_DATE_TIME,
                DateTimeFormatter.ISO_ZONED_DATE_TIME,
                DateTimeFormatter.RFC_1123_DATE_TIME
        );
        for (DateTimeFormatter formatter : zoned) {
            try {
                if (formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME) {
                    return OffsetDateTime.parse(value, formatter).toLocalDateTime().format(TS);
                }
                return ZonedDateTime.parse(value, formatter).toLocalDateTime().format(TS);
            } catch (Exception ignore) {
            }
        }
        List<DateTimeFormatter> localDateTimeFormats = Arrays.asList(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH"),
                DateTimeFormatter.ofPattern("MMM d, yyyy, HH:mm 'UTC'", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMM d, yyyy, HH:mm:ss 'UTC'", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMMM d, yyyy, HH:mm 'UTC'", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMMM d, yyyy, HH:mm:ss 'UTC'", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMM d, yyyy, h:mm a", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMM d, yyyy, hh:mm a", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMMM d, yyyy, h:mm a", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMMM d, yyyy, hh:mm a", Locale.ENGLISH)
        );
        for (DateTimeFormatter formatter : localDateTimeFormats) {
            try {
                return LocalDateTime.parse(value, formatter).format(TS);
            } catch (Exception ignore) {
            }
        }
        List<DateTimeFormatter> localDateFormats = Arrays.asList(
                DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                DateTimeFormatter.ofPattern("MMM d, yyyy", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH)
        );
        for (DateTimeFormatter formatter : localDateFormats) {
            try {
                return LocalDate.parse(value, formatter).atStartOfDay().format(TS);
            } catch (Exception ignore) {
            }
        }
        value = value.replace("T", " ").replace("Z", "");
        if (value.length() >= 19) {
            try {
                return LocalDateTime.parse(value.substring(0, 19), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).format(TS);
            } catch (Exception ignore) {
            }
        }
        return "";
    }

    private String encodeUtf8(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            return value;
        }
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (StringUtils.isNotBlank(value)) {
                return ExternalSourceUtils.safe(value);
            }
        }
        return "";
    }

    private String attr(Document doc, String css, String attr) {
        Element element = doc.selectFirst(css);
        return element == null ? "" : element.attr(attr);
    }

    private String text(Element element) {
        return element == null ? "" : ExternalSourceUtils.safe(element.text());
    }

    private String text(Object value) {
        return value == null ? "" : ExternalSourceUtils.safe(String.valueOf(value));
    }

    private int number(Object value) {
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception e) {
            return 0;
        }
    }

    private Map<String, Long> aggregateByStatus(List<CountRow> rows) {
        Map<String, Long> totals = new LinkedHashMap<String, Long>();
        if (rows == null) {
            return totals;
        }
        for (CountRow row : rows) {
            String key = ExternalSourceUtils.safe(row.key2);
            if (StringUtils.isBlank(key)) {
                key = "UNKNOWN";
            }
            totals.put(key, totals.getOrDefault(key, 0L) + (row.total == null ? 0L : row.total));
        }
        return totals;
    }

    private long sumTotals(List<CountRow> rows) {
        long total = 0L;
        if (rows == null) {
            return total;
        }
        for (CountRow row : rows) {
            total += row.total == null ? 0L : row.total;
        }
        return total;
    }

    private void sleepQuietly(long millis) {
        if (millis <= 0L) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String defaultIfBlank(String value, String fallback) {
        return StringUtils.isBlank(value) ? fallback : value.trim();
    }

    private List<String> collectMatches(String text, Pattern pattern) {
        List<String> result = new ArrayList<String>();
        if (StringUtils.isBlank(text) || pattern == null) {
            return result;
        }
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            result.add(matcher.group());
        }
        return result;
    }

    private String extractFirstRegex(String text, String regex) {
        if (StringUtils.isBlank(text) || StringUtils.isBlank(regex)) {
            return "";
        }
        Matcher matcher = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(text);
        if (!matcher.find()) {
            return "";
        }
        if (matcher.groupCount() >= 1) {
            return ExternalSourceUtils.safe(matcher.group(1));
        }
        return ExternalSourceUtils.safe(matcher.group());
    }

    private String extractFirstRegex(String text, Pattern pattern) {
        if (StringUtils.isBlank(text) || pattern == null) {
            return "";
        }
        Matcher matcher = pattern.matcher(text);
        if (!matcher.find()) {
            return "";
        }
        if (matcher.groupCount() >= 1) {
            return ExternalSourceUtils.safe(matcher.group(1));
        }
        return ExternalSourceUtils.safe(matcher.group());
    }

    private String selectJsonLd(Document doc) {
        if (doc == null) {
            return "";
        }
        for (Element script : doc.select("script[type=application/ld+json]")) {
            String json = firstNonBlank(script.data(), script.html());
            if (StringUtils.containsIgnoreCase(json, "\"headline\"") || StringUtils.containsIgnoreCase(json, "\"@type\":\"Article\"")) {
                return json;
            }
        }
        return "";
    }

    private String extractJsonLdField(String json, String field) {
        return extractFirstRegex(json, "\"" + Pattern.quote(field) + "\"\\s*:\\s*\"([^\"]+)\"");
    }

    private String extractJsonLdAuthorName(String json) {
        return extractFirstRegex(json, "\"author\"\\s*:\\s*\\{[^\\}]*?\"name\"\\s*:\\s*\"([^\"]+)\"");
    }

    private boolean isGitHubRepoPath(String href) {
        if (StringUtils.isBlank(href) || !GITHUB_HTML_REPO_PATH_PATTERN.matcher(href).matches()) {
            return false;
        }
        String trimmed = href.substring(1);
        int slash = trimmed.indexOf('/');
        if (slash <= 0) {
            return false;
        }
        String owner = trimmed.substring(0, slash).toLowerCase(Locale.ROOT);
        return !GITHUB_NON_REPO_ROOTS.contains(owner);
    }

    private static final class GitHubRepoSnapshot {
        private String fullName;
        private String htmlUrl;
        private String author;
        private String description;
        private String publishedAt;
        private String updatedAt;
        private String defaultBranch;
        private String stars;
        private String forks;
        private String language;
        private Object topics;
        private String readme;
        private String fallbackMode;
    }
}
