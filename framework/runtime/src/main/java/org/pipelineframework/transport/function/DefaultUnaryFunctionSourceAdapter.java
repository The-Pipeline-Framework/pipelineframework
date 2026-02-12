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

package org.pipelineframework.transport.function;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import io.smallrye.mutiny.Multi;

/**
 * Default unary source adapter that wraps one inbound event into one trace envelope.
 *
 * @param <I> inbound payload type
 */
public final class DefaultUnaryFunctionSourceAdapter<I> implements FunctionSourceAdapter<I, I> {
    private final String payloadModel;
    private final String payloadModelVersion;

    /**
     * Creates a source adapter.
     *
     * @param payloadModel payload model id
     * @param payloadModelVersion payload model version
     */
    public DefaultUnaryFunctionSourceAdapter(String payloadModel, String payloadModelVersion) {
        this.payloadModel = normalizeOrDefault(payloadModel, "unknown.input");
        this.payloadModelVersion = normalizeOrDefault(payloadModelVersion, "v1");
    }

    @Override
    public Multi<TraceEnvelope<I>> adapt(I event, FunctionTransportContext context) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(context, "context must not be null");
        String traceId = deriveTraceId(context.requestId());
        String itemId = UUID.randomUUID().toString();
        String idempotencyKey = resolveIdempotencyKey(context, traceId, itemId);

        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("functionName", normalizeOrDefault(context.functionName(), ""));
        meta.put("stage", normalizeOrDefault(context.stage(), ""));
        meta.put("requestId", normalizeOrDefault(context.requestId(), ""));
        if (context.attributes() != null && !context.attributes().isEmpty()) {
            context.attributes().forEach((key, value) -> {
                if (!"functionName".equals(key)
                    && !"stage".equals(key)
                    && !"requestId".equals(key)) {
                    meta.put(key, normalizeOrDefault(value, ""));
                }
            });
        }

        TraceEnvelope<I> envelope = new TraceEnvelope<>(
            traceId,
            null,
            itemId,
            null,
            payloadModel,
            payloadModelVersion,
            idempotencyKey,
            event,
            null,
            meta);
        return Multi.createFrom().item(envelope);
    }

    private static String normalizeOrDefault(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }

    private static String deriveTraceId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return requestId.trim();
    }

    private String resolveIdempotencyKey(FunctionTransportContext context, String traceId, String itemId) {
        if (context.idempotencyPolicy() == IdempotencyPolicy.EXPLICIT) {
            return context.explicitIdempotencyKey()
                .orElse(traceId + ":" + payloadModel + ":" + itemId);
        }
        return traceId + ":" + payloadModel + ":" + itemId;
    }
}
