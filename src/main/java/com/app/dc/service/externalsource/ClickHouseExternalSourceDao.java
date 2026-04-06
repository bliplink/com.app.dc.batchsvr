package com.app.dc.service.externalsource;

import com.app.common.db.ClickHouseDBUtils;
import com.app.dc.service.externalsource.ExternalSourceModels.CountRow;
import com.app.dc.service.externalsource.ExternalSourceModels.NormRecord;
import com.app.dc.service.externalsource.ExternalSourceModels.RawRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
@Slf4j
public class ClickHouseExternalSourceDao {

    @Autowired(required = false)
    private ClickHouseDBUtils clickHouseDBUtils;

    @Value("${strategy.source.raw.table:dc.strategy_source_raw}")
    private String rawTable;

    @Value("${strategy.source.norm.table:dc.strategy_source_norm}")
    private String normTable;

    public boolean ready() {
        return clickHouseDBUtils != null && StringUtils.isNotBlank(clickHouseDBUtils.getDbSourceName());
    }

    public boolean existsSameRawContent(String sourceType, String externalId, String contentHash) {
        if (!ready()) {
            return false;
        }
        String sql = "select count() as total from " + safeRawTable()
                + " where source_type=? and external_id=? and content_hash=? limit 1";
        try {
            List<CountRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{sourceType, externalId, contentHash}, CountRow.class);
            return rows != null && !rows.isEmpty() && rows.get(0).total != null && rows.get(0).total > 0;
        } catch (Exception e) {
            log.error("existsSameRawContent error, sourceType:{}, externalId:{}", sourceType, externalId, e);
            return false;
        }
    }

    public void insertRaw(RawRecord row) {
        if (!ready() || row == null) {
            return;
        }
        String sql = "insert into " + safeRawTable()
                + " (id, source_name, source_url, source_type, title, category_raw, logic_raw, market_raw, symbol_scope_raw,"
                + " crawl_time, source_hash, payload, status, site_name, external_id, canonical_url, author, published_time,"
                + " update_time, content_hash, content, create_time)"
                + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            ClickHouseDBUtils.update(sql, new Object[]{
                    row.id,
                    row.sourceName,
                    row.sourceUrl,
                    row.sourceType,
                    row.title,
                    "",
                    ExternalSourceUtils.summarizeLogic(row.content),
                    "crypto",
                    "ALL",
                    toDateTimeOrNull(row.createTime),
                    row.contentHash,
                    row.payload,
                    row.status,
                    row.siteName,
                    row.externalId,
                    row.canonicalUrl,
                    row.author,
                    toDateTimeOrNull(row.publishedTime),
                    toDateTimeOrNull(row.externalUpdateTime),
                    row.contentHash,
                    row.content,
                    toDateTimeOrNull(row.createTime)
            });
        } catch (Exception e) {
            log.error("insertRaw error, id:{}", row.id, e);
        }
    }

    public void markRawStatus(String id, String status, String payload) {
        if (!ready() || StringUtils.isBlank(id)) {
            return;
        }
        String sql = "insert into " + safeRawTable()
                + " (id, source_name, source_url, source_type, title, category_raw, logic_raw, market_raw, symbol_scope_raw,"
                + " crawl_time, source_hash, payload, status, site_name, external_id, canonical_url, author, published_time,"
                + " update_time, content_hash, content, create_time)"
                + " select id, source_name, source_url, source_type, title, category_raw, logic_raw, market_raw, symbol_scope_raw,"
                + " now(), source_hash, "
                + (payload == null ? "payload" : "'" + escape(payload) + "'")
                + ", '" + escape(status) + "', site_name, external_id, canonical_url, author, published_time,"
                + " update_time, content_hash, content, now()"
                + " from " + safeRawTable()
                + " where id='" + escape(id) + "' order by crawl_time desc limit 1";
        try {
            ClickHouseDBUtils.update(sql, new Object[]{});
        } catch (Exception e) {
            log.error("markRawStatus error, id:{}, status:{}", id, status, e);
        }
    }

    public List<RawRecord> pullLatestRawByStatus(String status, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String inner = "select "
                + "id as id,"
                + "argMax(source_type, crawl_time) as sourceType,"
                + "argMax(site_name, crawl_time) as siteName,"
                + "argMax(source_name, crawl_time) as sourceName,"
                + "argMax(external_id, crawl_time) as externalId,"
                + "argMax(canonical_url, crawl_time) as canonicalUrl,"
                + "argMax(source_url, crawl_time) as sourceUrl,"
                + "argMax(title, crawl_time) as title,"
                + "argMax(author, crawl_time) as author,"
                + "ifNull(toString(argMax(published_time, crawl_time)), '') as publishedTime,"
                + "ifNull(toString(argMax(update_time, crawl_time)), '') as externalUpdateTime,"
                + "argMax(content_hash, crawl_time) as contentHash,"
                + "argMax(content, crawl_time) as content,"
                + "argMax(status, crawl_time) as status,"
                + "toString(max(create_time)) as createTime,"
                + "argMax(payload, crawl_time) as payload,"
                + "max(crawl_time) as latestTime "
                + "from " + safeRawTable() + " group by id";
        String sql = "select id, sourceType, siteName, sourceName, externalId, canonicalUrl, sourceUrl, title, author,"
                + "publishedTime, externalUpdateTime, contentHash, content, status, createTime, payload "
                + "from (" + inner + ") latest where latest.status='" + escape(status) + "'"
                + " order by latest.latestTime asc limit " + Math.max(1, limit);
        try {
            List<RawRecord> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, RawRecord.class);
            return rows == null ? Collections.<RawRecord>emptyList() : rows;
        } catch (Exception e) {
            log.error("pullLatestRawByStatus error, status:{}", status, e);
            return Collections.emptyList();
        }
    }

    public boolean existsNormFingerprint(String scene, String strategyName, String fingerprint) {
        if (!ready()) {
            return false;
        }
        String sql = "select count() as total from " + safeNormTable()
                + " where scene=? and strategy_name=? and fingerprint=? limit 1";
        try {
            List<CountRow> rows = ClickHouseDBUtils.queryList(sql,
                    new Object[]{scene, strategyName, fingerprint},
                    CountRow.class);
            return rows != null && !rows.isEmpty() && rows.get(0).total != null && rows.get(0).total > 0;
        } catch (Exception e) {
            log.error("existsNormFingerprint error, strategyName:{}, fingerprint:{}", strategyName, fingerprint, e);
            return false;
        }
    }

    public void insertNorm(NormRecord row) {
        if (!ready() || row == null) {
            return;
        }
        String sql = "insert into " + safeNormTable()
                + " (id, raw_id, normalized_strategy_name, category, scene, logic_summary, logic_structured_json,"
                + " market_scope, symbol_scope, dedup_key, llm_model, normalize_time, payload, status, content,"
                + " strategy_name, description, create_time, normalized_title, source_type, site_name, canonical_url,"
                + " raw_content_hash, fingerprint, update_time)"
                + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            ClickHouseDBUtils.update(sql, new Object[]{
                    row.id,
                    row.rawId,
                    row.strategyName,
                    row.scene,
                    row.scene,
                    ExternalSourceUtils.summarizeLogic(row.content),
                    row.payload,
                    "crypto-usdt",
                    "ALL",
                    row.fingerprint,
                    "rule-based",
                    toDateTimeOrNull(row.updateTime),
                    row.payload,
                    row.status,
                    row.content,
                    row.strategyName,
                    row.description,
                    toDateTimeOrNull(row.createTime),
                    row.normalizedTitle,
                    row.sourceType,
                    row.siteName,
                    row.canonicalUrl,
                    row.rawContentHash,
                    row.fingerprint,
                    toDateTimeOrNull(row.updateTime)
            });
        } catch (Exception e) {
            log.error("insertNorm error, id:{}", row.id, e);
        }
    }

    public void markNormStatus(String id, String status, String payload) {
        if (!ready() || StringUtils.isBlank(id)) {
            return;
        }
        String sql = "insert into " + safeNormTable()
                + " (id, raw_id, normalized_strategy_name, category, scene, logic_summary, logic_structured_json,"
                + " market_scope, symbol_scope, dedup_key, llm_model, normalize_time, payload, status, content,"
                + " strategy_name, description, create_time, normalized_title, source_type, site_name, canonical_url,"
                + " raw_content_hash, fingerprint, update_time)"
                + " select id, raw_id, normalized_strategy_name, category, scene, logic_summary, logic_structured_json,"
                + " market_scope, symbol_scope, dedup_key, llm_model, now(), "
                + (payload == null ? "payload" : "'" + escape(payload) + "'")
                + ", '" + escape(status) + "', content, strategy_name, description, create_time, normalized_title,"
                + " source_type, site_name, canonical_url, raw_content_hash, fingerprint, now()"
                + " from " + safeNormTable()
                + " where id='" + escape(id) + "' order by normalize_time desc limit 1";
        try {
            ClickHouseDBUtils.update(sql, new Object[]{});
        } catch (Exception e) {
            log.error("markNormStatus error, id:{}, status:{}", id, status, e);
        }
    }

    public List<NormRecord> pullLatestNormByStatus(String status, int limit) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String inner = "select "
                + "id as id,"
                + "argMax(raw_id, normalize_time) as rawId,"
                + "argMax(source_type, normalize_time) as sourceType,"
                + "argMax(site_name, normalize_time) as siteName,"
                + "argMax(canonical_url, normalize_time) as canonicalUrl,"
                + "argMax(normalized_title, normalize_time) as normalizedTitle,"
                + "argMax(scene, normalize_time) as scene,"
                + "argMax(strategy_name, normalize_time) as strategyName,"
                + "argMax(description, normalize_time) as description,"
                + "argMax(content, normalize_time) as content,"
                + "argMax(raw_content_hash, normalize_time) as rawContentHash,"
                + "argMax(fingerprint, normalize_time) as fingerprint,"
                + "argMax(status, normalize_time) as status,"
                + "ifNull(toString(argMax(create_time, normalize_time)), '') as createTime,"
                + "ifNull(toString(max(update_time)), '') as updateTime,"
                + "argMax(payload, normalize_time) as payload,"
                + "max(normalize_time) as latestTime "
                + "from " + safeNormTable() + " group by id";
        String sql = "select id, rawId, sourceType, siteName, canonicalUrl, normalizedTitle, scene, strategyName, description, content,"
                + "rawContentHash, fingerprint, status, createTime, updateTime, payload "
                + "from (" + inner + ") latest where latest.status='" + escape(status) + "'"
                + " order by latest.latestTime asc limit " + Math.max(1, limit);
        try {
            List<NormRecord> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, NormRecord.class);
            return rows == null ? Collections.<NormRecord>emptyList() : rows;
        } catch (Exception e) {
            log.error("pullLatestNormByStatus error, status:{}", status, e);
            return Collections.emptyList();
        }
    }

    public List<CountRow> countLatestRawBySiteAndStatusSince(String since) {
        return countLatest("raw", since);
    }

    public List<CountRow> countLatestNormBySiteAndStatusSince(String since) {
        return countLatest("norm", since);
    }

    public List<CountRow> countLatestNormBySceneSince(String since) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String inner = "select id as id, argMax(scene, normalize_time) as key1, argMax(status, normalize_time) as status, max(normalize_time) as latestTime "
                + "from " + safeNormTable() + " group by id";
        String sql = "select key1 as key1, '' as key2, count() as total from (" + inner + ") latest "
                + "where latest.status='READY' and latest.latestTime >= toDateTime('" + escape(since) + "') group by key1 order by total desc";
        try {
            List<CountRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, CountRow.class);
            return rows == null ? Collections.<CountRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("countLatestNormBySceneSince error, since:{}", since, e);
            return Collections.emptyList();
        }
    }

    public List<NormRecord> loadLatestNormSamples(String status, int limit) {
        return pullLatestNormByStatus(status, limit);
    }

    private List<CountRow> countLatest(String type, String since) {
        if (!ready()) {
            return Collections.emptyList();
        }
        String table = "raw".equals(type) ? safeRawTable() : safeNormTable();
        String timeField = "raw".equals(type) ? "crawl_time" : "normalize_time";
        String inner = "select id as id, argMax(site_name, " + timeField + ") as key1, argMax(status, " + timeField + ") as key2, max(" + timeField + ") as latestTime "
                + "from " + table + " group by id";
        String sql = "select key1 as key1, key2 as key2, count() as total from (" + inner + ") latest "
                + "where latest.latestTime >= toDateTime('" + escape(since) + "') group by key1, key2 order by key1 asc, key2 asc";
        try {
            List<CountRow> rows = ClickHouseDBUtils.queryList(sql, new Object[]{}, CountRow.class);
            return rows == null ? Collections.<CountRow>emptyList() : rows;
        } catch (Exception e) {
            log.error("countLatest error, type:{}, since:{}", type, since, e);
            return Collections.emptyList();
        }
    }

    private Object toDateTimeOrNull(String value) {
        return StringUtils.isBlank(value) ? null : value;
    }

    private String safeRawTable() {
        return safeTable(rawTable, "dc.strategy_source_raw");
    }

    private String safeNormTable() {
        return safeTable(normTable, "dc.strategy_source_norm");
    }

    private String safeTable(String value, String fallback) {
        if (StringUtils.isBlank(value) || !value.trim().matches("[A-Za-z0-9_.]+")) {
            return fallback;
        }
        return value.trim();
    }

    private String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("'", "''");
    }
}
