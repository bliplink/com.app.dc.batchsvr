package com.app.dc.handler;

import com.app.common.utils.Consts;
import com.app.dc.service.BatchStart;
import com.gateway.connector.utils.JsonUtils;
import com.gw.common.utils.ContentHandler;
import com.gw.common.utils.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Service("dc.batch.external.run")
public class ExternalSourceRunHandler extends ContentHandler {

    @Autowired
    private BatchStart batchStart;

    @Override
    public Map<String, Object> handle(String topic, Message message, String content, Map<String, Object> map,
                                      boolean fromList) {
        Map<String, Object> result = new HashMap<String, Object>();
        try {
            @SuppressWarnings("unchecked")
            LinkedHashMap<String, Object> request = JsonUtils.Deserialize(content, LinkedHashMap.class);
            String action = request == null || request.get("action") == null ? "" : request.get("action").toString();
            String reportDate = request == null || request.get("reportDate") == null
                    ? "" : request.get("reportDate").toString().trim();
            Object data = runAction(action, reportDate);
            result.put(Consts.Code, Consts.SuccessCode);
            result.put(Consts.Msg, Consts.SuccessMsg);
            result.put(Consts.DATA, data);
        } catch (Exception e) {
            log.error("ExternalSourceRunHandler error, topic:{}, content:{}", topic, content, e);
            result.put(Consts.Code, Consts.NoKnowCode);
            result.put(Consts.Msg, e.getMessage());
        }
        return result;
    }

    private Object runAction(String action, String reportDate) {
        if ("system_report".equalsIgnoreCase(action) || "daily_report".equalsIgnoreCase(action)
                || "strategy_system_report".equalsIgnoreCase(action)) {
            return batchStart.runStrategySystemDailyReportJob(reportDate);
        }
        if ("runtime_report".equalsIgnoreCase(action) || "system_runtime_report".equalsIgnoreCase(action)
                || "strategy_system_runtime_report".equalsIgnoreCase(action)) {
            return batchStart.runStrategySystemRuntimeReportJob();
        }
        if ("discover_tradingview".equalsIgnoreCase(action)) {
            return batchStart.runTradingViewDiscoverJob();
        }
        if ("discover_fmz".equalsIgnoreCase(action)) {
            return batchStart.runFmzDiscoverJob();
        }
        if ("discover_github".equalsIgnoreCase(action)) {
            return batchStart.runGitHubDiscoverJob();
        }
        if ("normalize".equalsIgnoreCase(action)) {
            return batchStart.runExternalNormalizeJob();
        }
        if ("dispatch".equalsIgnoreCase(action)) {
            return batchStart.runExternalDispatchJob();
        }
        if ("digest".equalsIgnoreCase(action)) {
            return batchStart.runExternalDigestJob();
        }
        if ("all".equalsIgnoreCase(action)) {
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("tradingView", batchStart.runTradingViewDiscoverJob());
            data.put("fmz", batchStart.runFmzDiscoverJob());
            data.put("gitHub", batchStart.runGitHubDiscoverJob());
            data.put("normalize", batchStart.runExternalNormalizeJob());
            data.put("digest", batchStart.runExternalDigestJob());
            return data;
        }
        throw new IllegalArgumentException("unsupported action:" + action);
    }
}
