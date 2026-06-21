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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.grpc.StatusRuntimeException;
import org.pipelineframework.context.DispatchDeadlineValidator;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;
import org.pipelineframework.envelope.TpfEnvelope;
import org.pipelineframework.envelope.TpfEnvelopeCodec;
import org.pipelineframework.envelope.TpfEnvelopeControl;
import org.pipelineframework.step.NonRetryableException;

/**
 * Dispatches unary JSON envelope requests to remote operators.
 */
public class EnvelopeHttpRemoteOperatorClient {
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient httpClient;
    private final TpfEnvelopeCodec codec;

    public EnvelopeHttpRemoteOperatorClient() {
        this(HttpClient.newBuilder()
            .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build(), new TpfEnvelopeCodec());
    }

    EnvelopeHttpRemoteOperatorClient(HttpClient httpClient, TpfEnvelopeCodec codec) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.codec = Objects.requireNonNull(codec, "codec must not be null");
    }

    public <I, O> CompletionStage<O> invoke(
        String targetUrl,
        String step,
        String operatorId,
        I input,
        Class<O> outputType,
        Integer timeoutMs
    ) {
        Objects.requireNonNull(targetUrl, "targetUrl must not be null");
        Objects.requireNonNull(step, "step must not be null");
        Objects.requireNonNull(operatorId, "operatorId must not be null");
        Objects.requireNonNull(input, "input must not be null");
        Objects.requireNonNull(outputType, "outputType must not be null");
        PipelineContext pipelineContext = PipelineContextHolder.get();
        TransportDispatchMetadata transportMetadata = TransportDispatchMetadataHolder.get();
        return send(targetUrl, step, operatorId, input, outputType, timeoutMs, pipelineContext, transportMetadata);
    }

    private <I, O> CompletionStage<O> send(
        String targetUrl,
        String step,
        String operatorId,
        I input,
        Class<O> outputType,
        Integer timeoutMs,
        PipelineContext pipelineContext,
        TransportDispatchMetadata transportMetadata
    ) {
        try {
            EnvelopeDispatchMetadata metadata = currentTransportMetadata(input, operatorId, transportMetadata);
            Duration effectiveTimeout = computeEffectiveTimeout(metadata, timeoutMs, operatorId);
            TpfEnvelope requestEnvelope = codec.envelope(control(step, operatorId, pipelineContext, metadata),
                codec.jsonPayload(input, input.getClass().getName()));
            byte[] requestBody = codec.write(requestEnvelope);
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(targetUrl))
                .timeout(effectiveTimeout)
                .header("Content-Type", ProtobufHttpContentTypes.APPLICATION_TPF_ENVELOPE_JSON)
                .header("Accept", ProtobufHttpContentTypes.APPLICATION_TPF_ENVELOPE_JSON)
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody));
            applyContextHeaders(requestBuilder, pipelineContext, metadata);

            return httpClient.sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> decodeResponse(response, targetUrl, operatorId, outputType));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private <O> O decodeResponse(
        HttpResponse<byte[]> response,
        String targetUrl,
        String operatorId,
        Class<O> outputType
    ) {
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            TpfEnvelope responseEnvelope = codec.read(response.body() == null ? new byte[0] : response.body());
            return codec.readJsonPayload(responseEnvelope.payload(), outputType);
        }
        throw toRemoteFailure(response, targetUrl, operatorId);
    }

    private TpfEnvelopeControl control(
        String step,
        String operatorId,
        PipelineContext pipelineContext,
        EnvelopeDispatchMetadata metadata
    ) {
        return new TpfEnvelopeControl(
            pipelineContext == null || pipelineContext.versionTag() == null || pipelineContext.versionTag().isBlank()
                ? "unspecified"
                : pipelineContext.versionTag(),
            step,
            operatorId,
            controlContext(metadata),
            Map.of());
    }

    private Map<String, String> controlContext(EnvelopeDispatchMetadata metadata) {
        Map<String, String> context = new LinkedHashMap<>();
        putContext(context, "correlationId", metadata.correlationId());
        putContext(context, "executionId", metadata.executionId());
        putContext(context, "idempotencyKey", metadata.idempotencyKey());
        putContext(context, "retryAttempt", metadata.retryAttempt());
        putContext(context, "deadlineEpochMs", metadata.deadlineEpochMs());
        putContext(context, "dispatchTsEpochMs", metadata.dispatchTsEpochMs());
        putContext(context, "parentItemId", metadata.parentItemId());
        return Map.copyOf(context);
    }

    private void putContext(Map<String, String> context, String name, Optional<String> value) {
        value.ifPresent(actual -> context.put(name, actual));
    }

    private void putContext(Map<String, String> context, String name, OptionalLong value) {
        value.ifPresent(actual -> context.put(name, String.valueOf(actual)));
    }

    private void putContext(Map<String, String> context, String name, Object value) {
        context.put(name, String.valueOf(value));
    }

    private void applyContextHeaders(
        HttpRequest.Builder requestBuilder,
        PipelineContext pipelineContext,
        EnvelopeDispatchMetadata metadata
    ) {
        if (pipelineContext != null) {
            putHeader(requestBuilder, PipelineContextHeaders.VERSION, pipelineContext.versionTag());
            putHeader(requestBuilder, PipelineContextHeaders.REPLAY, pipelineContext.replayMode());
            putHeader(requestBuilder, PipelineContextHeaders.CACHE_POLICY, pipelineContext.cachePolicy());
        }
        putHeader(requestBuilder, PipelineContextHeaders.TPF_CORRELATION_ID, metadata.correlationId());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_EXECUTION_ID, metadata.executionId());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_IDEMPOTENCY_KEY, metadata.idempotencyKey());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_RETRY_ATTEMPT, metadata.retryAttempt());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS, metadata.deadlineEpochMs());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS, metadata.dispatchTsEpochMs());
        putHeader(requestBuilder, PipelineContextHeaders.TPF_PARENT_ITEM_ID, metadata.parentItemId());
    }

    private void putHeader(HttpRequest.Builder requestBuilder, String name, String value) {
        if (value != null && !value.isBlank()) {
            requestBuilder.header(name, value);
        }
    }

    private void putHeader(HttpRequest.Builder requestBuilder, String name, Optional<String> value) {
        value.ifPresent(actual -> putHeader(requestBuilder, name, actual));
    }

    private void putHeader(HttpRequest.Builder requestBuilder, String name, OptionalLong value) {
        value.ifPresent(actual -> requestBuilder.header(name, String.valueOf(actual)));
    }

    private void putHeader(HttpRequest.Builder requestBuilder, String name, Object value) {
        if (value != null) {
            requestBuilder.header(name, String.valueOf(value));
        }
    }

    private EnvelopeDispatchMetadata currentTransportMetadata(
        Object input,
        String operatorId,
        TransportDispatchMetadata existingMetadata
    ) {
        if (existingMetadata != null) {
            return new EnvelopeDispatchMetadata(
                Optional.ofNullable(existingMetadata.correlationId()),
                Optional.ofNullable(existingMetadata.executionId()),
                Optional.ofNullable(existingMetadata.idempotencyKey()),
                existingMetadata.retryAttempt() == null ? 0 : existingMetadata.retryAttempt(),
                existingMetadata.deadlineEpochMs() == null ? OptionalLong.empty() : OptionalLong.of(existingMetadata.deadlineEpochMs()),
                existingMetadata.dispatchTsEpochMs() == null ? Instant.now().toEpochMilli() : existingMetadata.dispatchTsEpochMs(),
                Optional.ofNullable(existingMetadata.parentItemId()));
        }
        String seed = UUID.nameUUIDFromBytes(codec.write(codec.envelope(
            new TpfEnvelopeControl("generated", operatorId, operatorId, Map.of("retryAttempt", "0"), Map.of()),
            codec.jsonPayload(input, input.getClass().getName())))).toString();
        long now = Instant.now().toEpochMilli();
        return new EnvelopeDispatchMetadata(
            Optional.of("remote-" + seed),
            Optional.of("remote-" + seed),
            Optional.of(operatorId + ":" + seed),
            0,
            OptionalLong.empty(),
            now,
            Optional.empty());
    }

    private Duration computeEffectiveTimeout(
        EnvelopeDispatchMetadata metadata,
        Integer timeoutMs,
        String operatorId
    ) {
        OptionalLong deadlineEpochMs = metadata.deadlineEpochMs();
        Long remainingDeadlineMs = deadlineEpochMs.isPresent() ? deadlineEpochMs.getAsLong() - Instant.now().toEpochMilli() : null;
        if (remainingDeadlineMs != null && remainingDeadlineMs <= 0) {
            try {
                DispatchDeadlineValidator.ensureNotExpired(deadlineEpochMs.getAsLong(), "remote-operator:" + operatorId);
            } catch (StatusRuntimeException ex) {
                throw new NonRetryableException("Remote operator deadline exceeded before dispatch", ex);
            }
        }

        Long effectiveMs = null;
        if (timeoutMs != null && deadlineEpochMs.isPresent()) {
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

    private record EnvelopeDispatchMetadata(
        Optional<String> correlationId,
        Optional<String> executionId,
        Optional<String> idempotencyKey,
        int retryAttempt,
        OptionalLong deadlineEpochMs,
        long dispatchTsEpochMs,
        Optional<String> parentItemId
    ) {
        private EnvelopeDispatchMetadata {
            correlationId = Objects.requireNonNull(correlationId, "correlationId must not be null");
            executionId = Objects.requireNonNull(executionId, "executionId must not be null");
            idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
            deadlineEpochMs = Objects.requireNonNull(deadlineEpochMs, "deadlineEpochMs must not be null");
            parentItemId = Objects.requireNonNull(parentItemId, "parentItemId must not be null");
            if (retryAttempt < 0) {
                throw new IllegalArgumentException("retryAttempt must not be negative");
            }
        }
    }
}
