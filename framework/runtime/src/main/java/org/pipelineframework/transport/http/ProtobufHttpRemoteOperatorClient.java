/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.transport.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.google.rpc.Code;
import com.google.rpc.Status;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.pipelineframework.context.DispatchDeadlineValidator;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;
import org.pipelineframework.step.NonRetryableException;

/**
 * Dispatches unary Protobuf-over-HTTP requests to remote operators.
 */
@ApplicationScoped
public class ProtobufHttpRemoteOperatorClient {
    private static final Logger LOG = Logger.getLogger(ProtobufHttpRemoteOperatorClient.class);
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient httpClient;

    public ProtobufHttpRemoteOperatorClient() {
        this(HttpClient.newBuilder()
            .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build());
    }

    ProtobufHttpRemoteOperatorClient(HttpClient httpClient) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    }

    public Uni<byte[]> invoke(String targetUrl, String operatorId, byte[] requestBody, Integer timeoutMs) {
        Objects.requireNonNull(targetUrl, "targetUrl must not be null");
        Objects.requireNonNull(operatorId, "operatorId must not be null");
        Objects.requireNonNull(requestBody, "requestBody must not be null");
        PipelineContext pipelineContext = PipelineContextHolder.get();
        TransportDispatchMetadata transportMetadata = TransportDispatchMetadataHolder.get();
        return Uni.createFrom().item(() -> send(targetUrl, operatorId, requestBody, timeoutMs, pipelineContext, transportMetadata))
            .runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    private byte[] send(
        String targetUrl,
        String operatorId,
        byte[] requestBody,
        Integer timeoutMs,
        PipelineContext pipelineContext,
        TransportDispatchMetadata transportMetadata
    ) {
        try {
            TransportDispatchMetadata metadata = currentTransportMetadata(requestBody, operatorId, transportMetadata);
            Duration effectiveTimeout = computeEffectiveTimeout(metadata, timeoutMs, operatorId);

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(targetUrl))
                .timeout(effectiveTimeout)
                .header("Content-Type", ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF)
                .header("Accept", ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF)
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody));
            applyContextHeaders(requestBuilder, pipelineContext, metadata);

            HttpResponse<byte[]> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return response.body() == null ? new byte[0] : response.body();
            }
            throw toRemoteFailure(response, targetUrl, operatorId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Remote operator invocation was interrupted", e);
        } catch (HttpTimeoutException e) {
            throw new IllegalStateException("Remote operator invocation timed out", e);
        } catch (IOException e) {
            throw new IllegalStateException("Remote operator invocation failed", e);
        }
    }

    private void applyContextHeaders(
        HttpRequest.Builder requestBuilder,
        PipelineContext pipelineContext,
        TransportDispatchMetadata metadata
    ) {
        if (pipelineContext != null) {
            putHeader(requestBuilder, PipelineContextHeaders.VERSION, pipelineContext.versionTag());
            putHeader(requestBuilder, PipelineContextHeaders.REPLAY, pipelineContext.replayMode());
            putHeader(requestBuilder, PipelineContextHeaders.CACHE_POLICY, pipelineContext.cachePolicy());
        }
        putHeader(requestBuilder, PipelineContextHeaders.TPF_CORRELATION_ID, metadata.correlationId());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_EXECUTION_ID, metadata.executionId());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_IDEMPOTENCY_KEY, metadata.idempotencyKey());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_RETRY_ATTEMPT, stringify(metadata.retryAttempt()));
        putHeader(requestBuilder, PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS, stringify(metadata.deadlineEpochMs()));
        putHeader(requestBuilder, PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS, stringify(metadata.dispatchTsEpochMs()));
        putHeader(requestBuilder, PipelineContextHeaders.TPF_PARENT_ITEM_ID, metadata.parentItemId());
    }

    private void putHeader(HttpRequest.Builder requestBuilder, String name, String value) {
        if (value != null && !value.isBlank()) {
            requestBuilder.header(name, value);
        }
    }

    private String stringify(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private TransportDispatchMetadata currentTransportMetadata(
        byte[] requestBody,
        String operatorId,
        TransportDispatchMetadata existingMetadata
    ) {
        TransportDispatchMetadata metadata = existingMetadata;
        if (metadata != null) {
            return metadata;
        }
        String seed = UUID.nameUUIDFromBytes(requestBody).toString();
        String invocationId = UUID.randomUUID().toString();
        long now = Instant.now().toEpochMilli();
        return new TransportDispatchMetadata(
            "remote-" + seed,
            "remote-" + seed,
            operatorId + ":" + seed + ":" + invocationId,
            0,
            null,
            now,
            null);
    }

    private Duration computeEffectiveTimeout(
        TransportDispatchMetadata metadata,
        Integer timeoutMs,
        String operatorId
    ) {
        Long deadlineEpochMs = metadata == null ? null : metadata.deadlineEpochMs();
        Long remainingDeadlineMs = deadlineEpochMs == null ? null : deadlineEpochMs - Instant.now().toEpochMilli();
        if (remainingDeadlineMs != null && remainingDeadlineMs <= 0) {
            try {
                DispatchDeadlineValidator.ensureNotExpired(deadlineEpochMs, "remote-operator:" + operatorId);
            } catch (StatusRuntimeException ex) {
                throw new NonRetryableException("Remote operator deadline exceeded before dispatch", ex);
            }
        }

        Long effectiveMs = null;
        if (timeoutMs != null && deadlineEpochMs != null) {
            effectiveMs = Math.min(timeoutMs.longValue(), remainingDeadlineMs);
        } else if (timeoutMs != null) {
            effectiveMs = timeoutMs.longValue();
        } else if (remainingDeadlineMs != null) {
            effectiveMs = remainingDeadlineMs;
        }

        if (effectiveMs == null) {
            return DEFAULT_REQUEST_TIMEOUT;
        }
        if (effectiveMs <= 0) {
            throw new NonRetryableException("Remote operator deadline exceeded before dispatch");
        }
        return Duration.ofMillis(effectiveMs);
    }

    private RuntimeException toRemoteFailure(HttpResponse<byte[]> response, String targetUrl, String operatorId) {
        byte[] body = response.body() == null ? new byte[0] : response.body();
        if (body.length > 0) {
            try {
                Status status = Status.parseFrom(body);
                Code code = Code.forNumber(status.getCode());
                String codeLabel = code == null ? "code=" + status.getCode() : code.name();
                String message = "Remote operator '" + operatorId + "' failed with HTTP " + response.statusCode()
                    + " at " + targetUrl + " (" + codeLabel + ": " + status.getMessage() + ")";
                if (ProtobufHttpStatusMapper.isRetryable(status)) {
                    return new IllegalStateException(message);
                }
                return new NonRetryableException(message);
            } catch (IOException ignored) {
                LOG.debugf("Remote operator '%s' failure body was not a protobuf Status envelope", operatorId);
            }
        }
        String responseBody = new String(body, StandardCharsets.UTF_8);
        if (responseBody.length() > 512) {
            responseBody = responseBody.substring(0, 512) + "...";
        }
        String message = "Remote operator '" + operatorId + "' failed with HTTP " + response.statusCode()
            + " at " + targetUrl + " (" + responseBody + ")";
        int statusCode = response.statusCode();
        if (statusCode >= 400 && statusCode < 500 && statusCode != 408 && statusCode != 429) {
            return new NonRetryableException(message);
        }
        return new IllegalStateException(message);
    }
}
