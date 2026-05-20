package org.pipelineframework.awaitable;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Await transport adapter that dispatches one interaction to an external webhook endpoint.
 */
@ApplicationScoped
public class WebhookAwaitTransportAdapter implements AwaitTransportAdapter<Object> {

    private static final String DEFAULT_COMPLETION_PATH = "/pipeline/interactions/complete";

    private final HttpClient httpClient;

    @Inject
    AwaitResumeTokenService resumeTokenService;

    public WebhookAwaitTransportAdapter() {
        this(HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build());
    }

    WebhookAwaitTransportAdapter(HttpClient httpClient) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    }

    @Override
    public String type() {
        return "webhook";
    }

    @Override
    public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
        Objects.requireNonNull(request, "request must not be null");
        AwaitStepDescriptor descriptor = request.descriptor();
        AwaitInteractionRecord interaction = request.interaction();
        String url = requiredUrl(descriptor.transportConfig());
        String method = stringValue(descriptor.transportConfig().get("method"), "POST").toUpperCase(Locale.ROOT);
        String token = resumeTokenService.sign(interaction, System.currentTimeMillis());
        Map<String, Object> envelope = envelope(descriptor, interaction, request.payload(), token);
        String body;
        URI uri;
        Duration timeout;
        try {
            uri = URI.create(url);
            timeout = requestTimeout(descriptor.transportConfig());
            body = PipelineJson.mapper().writeValueAsString(envelope);
        } catch (Exception e) {
            return Uni.createFrom().failure(dispatchConfigurationFailure(url, e));
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
            .timeout(timeout)
            .method(method, HttpRequest.BodyPublishers.ofString(body));
        applyHeaders(builder, descriptor.transportConfig());
        CompletionStage<HttpResponse<String>> response =
            httpClient.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString());
        return Uni.createFrom().completionStage(response)
            .onItem().transform(httpResponse -> {
                int status = httpResponse.statusCode();
                if (status < 200 || status >= 300) {
                    throw new IllegalStateException("Webhook await dispatch failed with HTTP " + status);
                }
                return new AwaitDispatchResult(Map.of(
                    "adapter", type(),
                    "url", url,
                    "statusCode", status,
                    "dispatchedAtEpochMs", System.currentTimeMillis()));
            });
    }

    private Map<String, Object> envelope(
        AwaitStepDescriptor descriptor,
        AwaitInteractionRecord interaction,
        Object payload,
        String resumeToken) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("tenantId", interaction.tenantId());
        envelope.put("interactionId", interaction.interactionId());
        envelope.put("correlationId", interaction.correlationId());
        envelope.put("resumeToken", resumeToken);
        envelope.put("executionId", interaction.executionId());
        envelope.put("stepId", interaction.stepId());
        envelope.put("outputType", interaction.outputType());
        envelope.put("deadlineEpochMs", interaction.deadlineEpochMs());
        envelope.put("requestPayload", payload);
        Object callbackObj = descriptor.transportConfig().get("callback");
        if (callbackObj instanceof Map<?, ?> callbackMap) {
            Map<String, Object> callback = stringifyMap(callbackMap);
            String completionUrl = stringValue(callback.get("completionUrl"), null);
            if (completionUrl != null && !completionUrl.isBlank()) {
                callback.put("completionUrl", completionUrl.trim());
            } else {
                String baseUrl = stringValue(callback.get("baseUrl"), null);
                if (baseUrl != null && !baseUrl.isBlank()) {
                    String completionPath = stringValue(callback.get("completionPath"), DEFAULT_COMPLETION_PATH);
                    callback.put("completionUrl", trimTrailingSlash(baseUrl.trim()) + normalizePath(completionPath));
                }
            }
            envelope.put("callback", callback);
        }
        return envelope;
    }

    private static String requiredUrl(Map<String, Object> config) {
        Object request = config.get("request");
        if (request instanceof Map<?, ?> requestMap) {
            String url = stringValue(requestMap.get("url"), null);
            if (url != null && !url.isBlank()) {
                return url;
            }
        }
        Object dispatch = config.get("dispatch");
        if (dispatch instanceof Map<?, ?> dispatchMap) {
            String url = stringValue(dispatchMap.get("url"), null);
            if (url != null && !url.isBlank()) {
                return url;
            }
        }
        String url = stringValue(config.get("url"), null);
        if (url != null && !url.isBlank()) {
            return url;
        }
        throw new IllegalArgumentException("webhook await transport requires request.url, dispatch.url, or url");
    }

    private static Duration requestTimeout(Map<String, Object> config) {
        Object timeout = config.get("timeout");
        if (timeout instanceof String text && !text.isBlank()) {
            try {
                return Duration.parse(text);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("webhook await transport timeout must be an ISO-8601 duration: " + text, e);
            }
        }
        Object millis = config.get("timeoutMillis");
        if (millis instanceof Number number) {
            return Duration.ofMillis(number.longValue());
        }
        return Duration.ofSeconds(30);
    }

    private static IllegalArgumentException dispatchConfigurationFailure(String url, Exception cause) {
        if (cause instanceof IllegalArgumentException illegalArgumentException) {
            return new IllegalArgumentException(
                "Invalid webhook await dispatch configuration for url '" + url + "': " + illegalArgumentException.getMessage(),
                illegalArgumentException);
        }
        return new IllegalArgumentException("Failed preparing webhook await dispatch envelope for url '" + url + "'", cause);
    }

    private static void applyHeaders(HttpRequest.Builder builder, Map<String, Object> config) {
        boolean hasContentType = false;
        Object headers = config.get("headers");
        if (headers instanceof Map<?, ?> headerMap) {
            for (Map.Entry<?, ?> entry : headerMap.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                String name = entry.getKey().toString();
                if ("content-type".equalsIgnoreCase(name)) {
                    hasContentType = true;
                }
                builder.header(name, entry.getValue().toString());
            }
        }
        if (!hasContentType) {
            builder.header("Content-Type", "application/json");
        }
    }

    private static Map<String, Object> stringifyMap(Map<?, ?> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (entry.getKey() != null) {
                result.put(entry.getKey().toString(), entry.getValue());
            }
        }
        return result;
    }

    private static String stringValue(Object value, String fallback) {
        return value == null ? fallback : value.toString();
    }

    private static String trimTrailingSlash(String value) {
        String result = value;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static String normalizePath(String value) {
        String path = value == null || value.isBlank() ? DEFAULT_COMPLETION_PATH : value.trim();
        return path.startsWith("/") ? path : "/" + path;
    }
}
