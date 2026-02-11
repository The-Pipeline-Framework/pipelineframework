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
 * Source adapter for streaming ingress events represented as {@code Multi<I>}.
 *
 * @param <I> payload type
 */
public final class MultiFunctionSourceAdapter<I> implements FunctionSourceAdapter<Multi<I>, I> {
    private final String payloadModel;
    private final String payloadModelVersion;

    /**
     * Creates an adapter.
     *
     * @param payloadModel payload model
     * @param payloadModelVersion payload model version
     */
    public MultiFunctionSourceAdapter(String payloadModel, String payloadModelVersion) {
        this.payloadModel = normalizeOrDefault(payloadModel, "unknown.input");
        this.payloadModelVersion = normalizeOrDefault(payloadModelVersion, "v1");
    }

    @Override
    public Multi<TraceEnvelope<I>> adapt(Multi<I> event, FunctionTransportContext context) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(context, "context must not be null");
        String traceId = normalizeOrDefault(context.requestId(), UUID.randomUUID().toString());
        Map<String, String> meta = buildMeta(context);

        return event.onItem().transform(item -> new TraceEnvelope<>(
            traceId,
            null,
            UUID.randomUUID().toString(),
            null,
            payloadModel,
            payloadModelVersion,
            traceId + ":" + payloadModel + ":" + UUID.randomUUID(),
            item,
            null,
            meta
        ));
    }

    private static Map<String, String> buildMeta(FunctionTransportContext context) {
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("functionName", normalizeOrDefault(context.functionName(), ""));
        meta.put("stage", normalizeOrDefault(context.stage(), ""));
        meta.put("requestId", normalizeOrDefault(context.requestId(), ""));
        if (context.attributes() != null && !context.attributes().isEmpty()) {
            context.attributes().forEach((key, value) -> {
                if (!"functionName".equals(key)
                    && !"stage".equals(key)
                    && !"requestId".equals(key)) {
                    meta.put(key, value);
                }
            });
        }
        return meta;
    }

    private static String normalizeOrDefault(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }
}
