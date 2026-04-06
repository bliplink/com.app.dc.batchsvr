package com.app.dc.service.externalsource;

import com.app.common.utils.JsonUtils;
import com.app.dc.service.externalsource.ExternalSourceModels.CountRow;
import com.app.dc.service.externalsource.ExternalSourceModels.NormRecord;
import com.app.dc.service.externalsource.ExternalSourceModels.RawRecord;
import com.app.dc.service.externalsource.ExternalSourceModels.SourceType;
import com.app.dc.utils.INDSvrClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

@Service
@Slf4j
public class ExternalSourceFacade {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private ClickHouseExternalSourceDao externalSourceDao;

    @Autowired
    private ExternalSourceHttpSupport httpSupport;

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
        Set<String> detailUrls = new LinkedHashSet<String>();
        for (int page = 1; page <= Math.max(1, fmzPageLimit); page++) {
            Document doc = loadFirstDocument(new String[]{
                    "https://www.fmz.com/strategy?p=" + page,
                    "https://www.fmz.com/strategy?page=" + page,
                    page == 1 ? "https://www.fmz.com/strategy" : "https://www.fmz.com/strategy"
            });
            if (doc == null) {
                continue;
            }
            detailUrls.addAll(extractLinks(doc.select("a[href]"), "https://www.fmz.com", "/strategy/"));
        }
        for (String url : detailUrls) {
            discovered += captureFmzDetail(url);
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
        for (String query : resolveGithubQueries()) {
            if (remaining <= 0) {
                break;
            }
            int perPage = Math.min(remaining, 20);
            String q = query
                    + " archived:false fork:false stars:>=" + Math.max(0, githubMinStars)
                    + " pushed:>=" + LocalDate.now().minusDays(Math.max(1, githubQueryWindowDays));
            String url = "https://api.github.com/search/repositories?q="
                    + encodeUtf8(q)
                    + "&sort=updated&order=desc&per_page=" + perPage + "&page=1";
            try {
                Map<String, Object> response = httpSupport.getJsonObject(url, githubHeaders("application/vnd.github+json"));
                Object itemsObj = response.get("items");
                if (!(itemsObj instanceof List)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> items = (List<Map<String, Object>>) itemsObj;
                for (Map<String, Object> item : items) {
                    if (remaining <= 0) {
                        break;
                    }
                    String fullName = text(item.get("full_name"));
                    String htmlUrl = text(item.get("html_url"));
                    String title = fullName;
                    String author = text(item.get("owner") instanceof Map ? ((Map<?, ?>) item.get("owner")).get("login") : "");
                    String description = text(item.get("description"));
                    String updatedAt = normalizeIsoDateTime(text(item.get("updated_at")));
                    String publishedAt = normalizeIsoDateTime(text(item.get("created_at")));
                    String defaultBranch = text(item.get("default_branch"));
                    StringBuilder content = new StringBuilder();
                    content.append(description);
                    if (enriched < Math.max(1, githubReadmeLimit)) {
                        String readme = fetchGitHubReadme(fullName);
                        if (StringUtils.isNotBlank(readme)) {
                            content.append("\nREADME: ").append(ExternalSourceUtils.truncate(readme, 8000));
                        }
                        if (StringUtils.isNotBlank(githubToken)) {
                            List<String> snippets = fetchGitHubFileSnippets(fullName);
                            for (String snippet : snippets) {
                                content.append("\nFILE: ").append(ExternalSourceUtils.truncate(snippet, 2500));
                            }
                        }
                        enriched++;
                    }
                    Map<String, Object> payload = new LinkedHashMap<String, Object>();
                    payload.put("sourceType", SourceType.GITHUB.name());
                    payload.put("siteName", SourceType.GITHUB.siteName());
                    payload.put("fullName", fullName);
                    payload.put("htmlUrl", htmlUrl);
                    payload.put("defaultBranch", defaultBranch);
                    payload.put("stars", item.get("stargazers_count"));
                    payload.put("forks", item.get("forks_count"));
                    payload.put("language", item.get("language"));
                    payload.put("topics", item.get("topics"));
                    payload.put("query", query);
                    payload.put("description", description);
                    discovered += persistRaw(SourceType.GITHUB, fullName, htmlUrl, htmlUrl, title, author,
                            publishedAt, updatedAt, content.toString(), payload);
                    remaining--;
                }
            } catch (Exception e) {
                log.warn("ExternalSourceFacade discoverGitHub query error, query:{}", query, e);
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
            try {
                NormRecord norm = buildNorm(raw);
                if (norm == null) {
                    externalSourceDao.markRawStatus(raw.id, "IGNORED", "{\"reason\":\"normalize_empty\"}");
                    continue;
                }
                if (externalSourceDao.existsNormFingerprint(norm.scene, norm.strategyName, norm.fingerprint)) {
                    norm.status = "DUPLICATE";
                } else {
                    norm.status = "READY";
                }
                externalSourceDao.insertNorm(norm);
                Map<String, Object> payload = new LinkedHashMap<String, Object>();
                payload.put("normId", norm.id);
                payload.put("normStatus", norm.status);
                payload.put("scene", norm.scene);
                payload.put("strategyName", norm.strategyName);
                externalSourceDao.markRawStatus(raw.id, "NORMALIZED", JsonUtils.Serializer(payload));
                processed++;
            } catch (Exception e) {
                log.error("ExternalSourceFacade normalize error, rawId:{}", raw.id, e);
                externalSourceDao.markRawStatus(raw.id, "ERROR", "{\"reason\":\"normalize_exception\"}");
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
            try {
                externalSourceDao.markNormStatus(row.id, "DISPATCHING", row.payload);
                LinkedHashMap<String, Object> request = new LinkedHashMap<String, Object>();
                request.put("sourceType", "EXTERNAL_SOURCE");
                request.put("sourceRef", row.id);
                request.put("scene", row.scene);
                request.put("description", row.description);
                request.put("strategyName", row.strategyName);
                request.put("provider", dispatchProvider);
                request.put("payload", JsonUtils.Deserialize(row.payload, LinkedHashMap.class));
                Map<String, Object> rsp = indSvrClient.requestStrategyCandidateGenerate(request);
                int code = number(rsp.get("code"));
                String msg = text(rsp.get("msg"));
                if (code == 0) {
                    externalSourceDao.markNormStatus(row.id, "GENERATED", JsonUtils.Serializer(rsp));
                } else if (StringUtils.containsIgnoreCase(msg, "skip")) {
                    externalSourceDao.markNormStatus(row.id, "SKIPPED", JsonUtils.Serializer(rsp));
                } else {
                    externalSourceDao.markNormStatus(row.id, "ERROR", JsonUtils.Serializer(rsp));
                }
                dispatched++;
            } catch (Exception e) {
                log.error("ExternalSourceFacade dispatch error, normId:{}", row.id, e);
                externalSourceDao.markNormStatus(row.id, "ERROR", "{\"reason\":\"dispatch_exception\"}");
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

        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("generatedAt", ExternalSourceUtils.nowText());
        summary.put("since", since);
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
            Files.write(mdPath, buildDigestMarkdown(summary, rawCounts, normCounts, sceneCounts, readySamples).getBytes(StandardCharsets.UTF_8));
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
        sb.append("- 统计起点: ").append(summary.get("since")).append("\n\n");
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
        for (NormRecord row : readySamples) {
            sb.append("- ").append(row.siteName).append(" | ").append(row.strategyName).append(" | ")
                    .append(row.scene).append(" | ").append(row.normalizedTitle).append("\n");
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
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
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
            String title = firstNonBlank(
                    attr(doc, "meta[property=og:title]", "content"),
                    attr(doc, "meta[name=twitter:title]", "content"),
                    doc.title());
            String content = firstNonBlank(
                    attr(doc, "meta[property=og:description]", "content"),
                    attr(doc, "meta[name=description]", "content"),
                    text(doc.selectFirst("main")),
                    text(doc.body()));
            String author = text(doc.selectFirst("a[href^=/u/]"));
            String publishedTime = attr(doc, "time[datetime]", "datetime");
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
            return persistRaw(SourceType.TRADINGVIEW, extractExternalId(canonicalUrl, "/script/"), canonicalUrl, url, title,
                    author, normalizeIsoDateTime(publishedTime), normalizeIsoDateTime(publishedTime), content, payload);
        } catch (Exception e) {
            log.warn("captureTradingViewDetail error, url:{}", url, e);
            return 0;
        }
    }

    private int captureFmzDetail(String url) {
        try {
            Document doc = httpSupport.getDocument(url);
            String title = firstNonBlank(
                    attr(doc, "meta[property=og:title]", "content"),
                    attr(doc, "meta[name=description]", "content"),
                    text(doc.selectFirst("h1")),
                    doc.title());
            String content = firstNonBlank(
                    attr(doc, "meta[property=og:description]", "content"),
                    text(doc.selectFirst("main")),
                    text(doc.body()));
            String author = firstNonBlank(text(doc.selectFirst("a[href*=/user/]")), text(doc.selectFirst(".author")));
            String publishedTime = attr(doc, "time[datetime]", "datetime");
            String canonicalUrl = firstNonBlank(attr(doc, "link[rel=canonical]", "href"), url);
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
            return persistRaw(SourceType.FMZ, extractExternalId(canonicalUrl, "/strategy/"), canonicalUrl, url, title,
                    author, normalizeIsoDateTime(publishedTime), normalizeIsoDateTime(publishedTime), content, payload);
        } catch (Exception e) {
            log.warn("captureFmzDetail error, url:{}", url, e);
            return 0;
        }
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
        String contentHash = ExternalSourceUtils.sha256(title + "\n" + content);
        boolean duplicate = externalSourceDao.existsSameRawContent(sourceType.name(), externalId, contentHash);
        RawRecord row = new RawRecord();
        row.id = "raw_" + ExternalSourceUtils.slug(sourceType.name().toLowerCase(Locale.ROOT), "src")
                + "_" + System.currentTimeMillis()
                + "_" + contentHash.substring(0, Math.min(12, contentHash.length()));
        row.sourceType = sourceType.name();
        row.siteName = sourceType.siteName();
        row.sourceName = sourceType.siteName();
        row.externalId = StringUtils.defaultIfBlank(externalId, ExternalSourceUtils.sha256(canonicalUrl));
        row.canonicalUrl = canonicalUrl;
        row.sourceUrl = sourceUrl;
        row.title = ExternalSourceUtils.truncate(title, 512);
        row.author = ExternalSourceUtils.truncate(author, 256);
        row.publishedTime = publishedTime;
        row.externalUpdateTime = updateTime;
        row.contentHash = contentHash;
        row.content = ExternalSourceUtils.truncate(content, 16000);
        row.status = duplicate ? "DUPLICATE" : "NEW";
        row.createTime = ExternalSourceUtils.nowText();
        row.payload = JsonUtils.Serializer(payload == null ? Collections.emptyMap() : payload);
        externalSourceDao.insertRaw(row);
        return duplicate ? 0 : 1;
    }

    private String fetchGitHubReadme(String fullName) {
        try {
            String url = "https://api.github.com/repos/" + fullName + "/readme";
            return httpSupport.getText(url, githubHeaders("application/vnd.github.raw+json"));
        } catch (Exception e) {
            log.debug("fetchGitHubReadme skip, fullName:{}", fullName, e);
            return "";
        }
    }

    private List<String> fetchGitHubFileSnippets(String fullName) {
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
        List<DateTimeFormatter> formatters = Arrays.asList(
                DateTimeFormatter.ISO_OFFSET_DATE_TIME,
                DateTimeFormatter.ISO_ZONED_DATE_TIME,
                DateTimeFormatter.RFC_1123_DATE_TIME,
                DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm z", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH")
        );
        for (DateTimeFormatter formatter : formatters) {
            try {
                if (formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME) {
                    return OffsetDateTime.parse(value, formatter).toLocalDateTime().format(TS);
                }
                if (formatter == DateTimeFormatter.ISO_ZONED_DATE_TIME || formatter == DateTimeFormatter.RFC_1123_DATE_TIME) {
                    return ZonedDateTime.parse(value, formatter).toLocalDateTime().format(TS);
                }
                return LocalDateTime.parse(value, formatter).format(TS);
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
}
