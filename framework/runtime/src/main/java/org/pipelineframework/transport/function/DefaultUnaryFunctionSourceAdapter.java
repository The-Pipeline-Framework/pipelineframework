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
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import io.smallrye.mutiny.Multi;

/**
 * Default unary source adapter that wraps one inbound event into one trace envelope.
 *
 * @param <I> inbound payload type
 */
public final class DefaultUnaryFunctionSourceAdapter<I> implements FunctionSourceAdapter<I, I> {
    private static final Logger LOG = Logger.getLogger(DefaultUnaryFunctionSourceAdapter.class.getName());
    private static final Set<String> RESERVED_KEYS = Set.of("functionName", "stage", "requestId");
    private final String payloadModel;
    private final String payloadModelVersion;

    /**
     * Creates a source adapter.
     *
     * @param payloadModel payload model id
     * @param payloadModelVersion payload model version
     */
    public DefaultUnaryFunctionSourceAdapter(String payloadModel, String payloadModelVersion) {
        this.payloadModel = AdapterUtils.normalizeOrDefault(payloadModel, "unknown.input");
        this.payloadModelVersion = AdapterUtils.normalizeOrDefault(payloadModelVersion, "v1");
    }

    @Override
    public Multi<TraceEnvelope<I>> adapt(I event, FunctionTransportContext context) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(context, "context must not be null");
        String traceId = AdapterUtils.deriveTraceId(context.requestId());
        String itemId = UUID.randomUUID().toString();
        String idempotencyKey = resolveIdempotencyKey(context, traceId);

        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("functionName", AdapterUtils.normalizeOrDefault(context.functionName(), ""));
        meta.put("stage", AdapterUtils.normalizeOrDefault(context.stage(), ""));
        meta.put("requestId", AdapterUtils.normalizeOrDefault(context.requestId(), ""));
        if (context.attributes() != null && !context.attributes().isEmpty()) {
            context.attributes().forEach((key, value) -> {
                if (!RESERVED_KEYS.contains(key)
                    && !key.startsWith("tpf.")) {
                    meta.put(key, AdapterUtils.normalizeOrDefault(value, ""));
                }
            });
        }

        TraceEnvelope<I> envelope = TraceEnvelope.rootWithMeta(
            traceId,
            itemId,
            payloadModel,
            payloadModelVersion,
            idempotencyKey,
            event,
            meta);
        return Multi.createFrom().item(envelope);
    }

    private String resolveIdempotencyKey(FunctionTransportContext context, String traceId) {
        if (context.idempotencyPolicy() == IdempotencyPolicy.EXPLICIT) {
            if (context.explicitIdempotencyKey().isEmpty()) {
                LOG.warning(() -> "IdempotencyPolicy.EXPLICIT configured but no explicit key provided; "
                    + "falling back to CONTEXT_STABLE key derivation.");
            }
        }
        String deterministicSuffix = AdapterUtils.normalizeOrDefault(context.requestId(), traceId);
        return IdempotencyKeyResolver.resolve(context, traceId, payloadModel, deterministicSuffix);
    }

}
