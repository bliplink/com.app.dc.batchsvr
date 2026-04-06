package com.app.dc.service.externalsource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class ExternalSourceHttpSupport {

    private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            + "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36";

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExternalSourceHttpSupport() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10000);
        factory.setReadTimeout(20000);
        this.restTemplate = new RestTemplate(factory);
    }

    public Document getDocument(String url) throws Exception {
        Connection connection = Jsoup.connect(url)
                .userAgent(USER_AGENT)
                .timeout(20000)
                .ignoreContentType(true)
                .followRedirects(true);
        return connection.get();
    }

    public String getText(String url, Map<String, String> headers) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("User-Agent", USER_AGENT);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpHeaders.add(entry.getKey(), entry.getValue());
            }
        }
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<Void>(httpHeaders), String.class);
        return response.getBody();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getJsonObject(String url, Map<String, String> headers) throws Exception {
        String body = getText(url, headers);
        if (body == null || body.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        return objectMapper.readValue(body, LinkedHashMap.class);
    }

    public List<Map<String, Object>> getJsonList(String url, Map<String, String> headers) throws Exception {
        String body = getText(url, headers);
        if (body == null || body.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<LinkedHashMap<String, Object>> rows = objectMapper.readValue(body,
                new TypeReference<List<LinkedHashMap<String, Object>>>() {
                });
        return (List<Map<String, Object>>) (List<?>) rows;
    }
}
