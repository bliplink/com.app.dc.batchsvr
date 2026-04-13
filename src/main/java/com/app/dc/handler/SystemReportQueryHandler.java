package com.app.dc.handler;

import com.app.common.utils.Consts;
import com.app.dc.service.systemreport.ClickHouseStrategySystemReportDao;
import com.app.dc.service.systemreport.ClickHouseStrategySystemReportDao.StrategySystemDailyReportRow;
import com.gateway.connector.utils.JsonUtils;
import com.gw.common.utils.ContentHandler;
import com.gw.common.utils.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service("dc.batch.system.report.query")
public class SystemReportQueryHandler extends ContentHandler {

    @Autowired(required = false)
    private ClickHouseStrategySystemReportDao dao;

    @Override
    public Map<String, Object> handle(String topic, Message message, String content, Map<String, Object> map,
                                      boolean fromList) {
        Map<String, Object> result = new HashMap<String, Object>();
        try {
            if (dao == null || !dao.ready()) {
                result.put(Consts.Code, Consts.NoKnowCode);
                result.put(Consts.Msg, "system report dao not ready");
                return result;
            }
            @SuppressWarnings("unchecked")
            LinkedHashMap<String, Object> request = JsonUtils.Deserialize(content, LinkedHashMap.class);
            String reportDate = request == null || request.get("reportDate") == null
                    ? "" : request.get("reportDate").toString().trim();
            int recentLimit = parsePositiveInt(request == null ? null : request.get("recentLimit"), 7);
            if (recentLimit > 60) {
                recentLimit = 60;
            }

            StrategySystemDailyReportRow row = StringUtils.isBlank(reportDate)
                    ? dao.loadLatestReport()
                    : dao.loadReportByDate(reportDate);

            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("requestedDate", reportDate);
            data.put("found", row != null);
            data.put("report", toReportMap(row));
            data.put("recentReports", toRecentList(dao.loadRecentReports(recentLimit)));
            result.put(Consts.Code, Consts.SuccessCode);
            result.put(Consts.Msg, Consts.SuccessMsg);
            result.put(Consts.DATA, data);
            log.info("SystemReportQueryHandler success, requestedDate:{}, found:{}, recentLimit:{}",
                    reportDate, row != null, recentLimit);
        } catch (Exception e) {
            log.error("SystemReportQueryHandler error, topic:{}, content:{}", topic, content, e);
            result.put(Consts.Code, Consts.NoKnowCode);
            result.put(Consts.Msg, e.getMessage());
        }
        return result;
    }

    private int parsePositiveInt(Object obj, int defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        try {
            int value = Integer.parseInt(String.valueOf(obj).trim());
            return value > 0 ? value : defaultValue;
        } catch (Exception ignore) {
            return defaultValue;
        }
    }

    private List<Map<String, Object>> toRecentList(List<StrategySystemDailyReportRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> list = new java.util.ArrayList<Map<String, Object>>();
        for (StrategySystemDailyReportRow row : rows) {
            if (row == null) {
                continue;
            }
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("id", row.id);
            item.put("reportDate", row.reportDate);
            item.put("generatedAt", row.generatedAt);
            item.put("htmlPath", row.htmlPath);
            item.put("jsonPath", row.jsonPath);
            list.add(item);
        }
        return list;
    }

    private Map<String, Object> toReportMap(StrategySystemDailyReportRow row) {
        if (row == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> report = new LinkedHashMap<String, Object>();
        report.put("id", row.id);
        report.put("reportDate", row.reportDate);
        report.put("generatedAt", row.generatedAt);
        report.put("htmlPath", row.htmlPath);
        report.put("jsonPath", row.jsonPath);
        report.put("summaryJson", row.summaryJson);
        report.put("payload", row.payload);
        report.put("summary", parseJsonObject(row.summaryJson));
        return report;
    }

    private Map<String, Object> parseJsonObject(String json) {
        if (StringUtils.isBlank(json)) {
            return Collections.emptyMap();
        }
        try {
            @SuppressWarnings("unchecked")
            LinkedHashMap<String, Object> data = JsonUtils.Deserialize(json, LinkedHashMap.class);
            return data == null ? Collections.<String, Object>emptyMap() : data;
        } catch (Exception e) {
            log.warn("SystemReportQueryHandler parse summary json failed");
            return Collections.emptyMap();
        }
    }
}
