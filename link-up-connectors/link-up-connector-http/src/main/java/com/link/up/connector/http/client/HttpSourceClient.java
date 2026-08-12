package com.link.up.connector.http.client;

import com.link.up.connector.http.config.HttpFormat;
import com.link.up.connector.http.config.HttpMethod;
import com.link.up.connector.http.config.HttpSourceConfig;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * HTTP 请求客户端。
 *
 * <p>封装 OkHttp，提供带重试的 HTTP 请求执行能力。
 */
public final class HttpSourceClient implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(HttpSourceClient.class);

    private static final MediaType JSON_MEDIA_TYPE =
            MediaType.parse("application/json; charset=utf-8");

    private final HttpSourceConfig config;
    private final OkHttpClient httpClient;

    public HttpSourceClient(HttpSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * 执行一次 HTTP 请求，返回响应体字符串。
     *
     * <p>包含重试逻辑：失败时按指数退避重试。
     *
     * @param effectiveHeaders 当前请求头（可能已替换分页占位符）
     * @param effectiveParams  当前请求参数（可能已替换分页占位符）
     * @param effectiveBody    当前请求体（可能已替换分页占位符）
     * @return 响应体文本
     */
    public String execute(
            Map<String, String> effectiveHeaders,
            Map<String, String> effectiveParams,
            String effectiveBody) throws IOException {

        IOException lastException = null;
        int maxAttempts = Math.max(1, config.getRetry() + 1);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return doExecute(effectiveHeaders, effectiveParams, effectiveBody);
            } catch (IOException e) {
                lastException = e;
                if (attempt < maxAttempts) {
                    long backoff = Math.min(
                            (long) config.getRetryBackoffMultiplierMs() * (1L << (attempt - 1)),
                            config.getRetryBackoffMaxMs());
                    LOG.warn("HTTP request failed (attempt {}/{}), retrying in {} ms: {}",
                            attempt, maxAttempts, backoff, e.getMessage());
                    sleep(backoff);
                }
            }
        }

        throw lastException;
    }

    private String doExecute(
            Map<String, String> effectiveHeaders,
            Map<String, String> effectiveParams,
            String effectiveBody) throws IOException {

        String url = buildUrl(effectiveParams);

        Request.Builder requestBuilder = new Request.Builder().url(url);

        // 设置请求头
        if (effectiveHeaders != null) {
            for (Map.Entry<String, String> entry : effectiveHeaders.entrySet()) {
                requestBuilder.header(entry.getKey(), entry.getValue());
            }
        }

        // 设置请求方法和请求体
        HttpMethod method = config.getMethod();
        if (method == HttpMethod.POST) {
            String bodyContent = effectiveBody != null ? effectiveBody : config.getBody();
            RequestBody requestBody;
            if (bodyContent != null && !bodyContent.isEmpty()) {
                String contentType = getContentType(effectiveHeaders);
                if ("application/x-www-form-urlencoded".equalsIgnoreCase(contentType)) {
                    requestBody = buildFormBody(bodyContent, effectiveParams);
                } else {
                    requestBody = RequestBody.create(bodyContent, JSON_MEDIA_TYPE);
                }
            } else {
                // POST 无 body 时发送空 JSON
                requestBody = RequestBody.create("{}", JSON_MEDIA_TYPE);
            }
            requestBuilder.post(requestBody);
        } else {
            requestBuilder.get();
        }

        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP request failed with status " + response.code()
                        + " for URL: " + url);
            }
            ResponseBody responseBody = response.body();
            return responseBody != null ? responseBody.string() : "";
        }
    }

    private String buildUrl(Map<String, String> effectiveParams) {
        StringBuilder urlBuilder = new StringBuilder(config.getUrl());

        // 合并静态 params 和分页 params
        Map<String, String> allParams = new LinkedHashMap<>();
        if (config.getParams() != null) {
            allParams.putAll(config.getParams());
        }
        if (effectiveParams != null) {
            allParams.putAll(effectiveParams);
        }

        if (!allParams.isEmpty()) {
            char separator = config.getUrl().contains("?") ? '&' : '?';
            for (Map.Entry<String, String> entry : allParams.entrySet()) {
                urlBuilder.append(separator)
                        .append(urlEncode(entry.getKey()))
                        .append('=')
                        .append(urlEncode(entry.getValue()));
                separator = '&';
            }
        }

        return urlBuilder.toString();
    }

    private RequestBody buildFormBody(
            String bodyContent,
            Map<String, String> effectiveParams) {
        FormBody.Builder formBuilder = new FormBody.Builder();

        // 解析 body 中的 form 参数（简单 key=value&key2=value2 格式）
        if (bodyContent != null && !bodyContent.isEmpty()) {
            for (String pair : bodyContent.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    formBuilder.add(
                            pair.substring(0, eq).trim(),
                            pair.substring(eq + 1).trim());
                }
            }
        }

        // 追加 params
        if (effectiveParams != null) {
            for (Map.Entry<String, String> entry : effectiveParams.entrySet()) {
                formBuilder.add(entry.getKey(), entry.getValue());
            }
        }

        return formBuilder.build();
    }

    private String getContentType(Map<String, String> headers) {
        if (headers == null) {
            return null;
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if ("content-type".equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (Exception e) {
            return value;
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }
}
