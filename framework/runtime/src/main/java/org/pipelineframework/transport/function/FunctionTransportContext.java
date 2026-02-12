/*
 * Copyright (c) 2023-2025 Mariano Barcia
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Context propagated at function transport boundaries.
 *
 * @param requestId external invocation/request identifier
 * @param functionName logical function name
 * @param stage logical stage (for example ingress, invoke-step, egress)
 * @param attributes free-form key/value attributes
 */
public record FunctionTransportContext(
    String requestId,
    String functionName,
    String stage,
    Map<String, String> attributes
) {
    private static final Logger LOG = Logger.getLogger(FunctionTransportContext.class.getName());
    public static final String ATTR_IDEMPOTENCY_POLICY = "tpf.idempotency.policy";
    public static final String ATTR_IDEMPOTENCY_KEY = "tpf.idempotency.key";

    /**
     * Creates a context with immutable attributes.
     */
    public FunctionTransportContext {
        Objects.requireNonNull(requestId, "requestId must not be null");
        functionName = functionName == null ? "" : functionName;
        stage = stage == null ? "" : stage;
        attributes = attributes == null
            ? Map.of()
            : Map.copyOf(new LinkedHashMap<>(attributes));
    }

    /**
     * Creates a basic context.
     *
     * @param requestId request id
     * @param functionName function name
     * @param stage stage name
     * @return context with no custom attributes
     */
    public static FunctionTransportContext of(String requestId, String functionName, String stage) {
        return new FunctionTransportContext(requestId, functionName, stage, Map.of());
    }

    /**
     * Resolves idempotency policy from context attributes.
     *
     * @return resolved policy; defaults to CONTEXT_STABLE when unset/unknown.
     *     Unknown values are logged and treated as CONTEXT_STABLE. Legacy RANDOM is accepted
     *     as compatibility alias and normalized to CONTEXT_STABLE.
     */
    public IdempotencyPolicy idempotencyPolicy() {
        String raw = attributes.get(ATTR_IDEMPOTENCY_POLICY);
        if (raw == null || raw.isBlank()) {
            return IdempotencyPolicy.CONTEXT_STABLE;
        }
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        if ("EXPLICIT".equals(normalized)) {
            return IdempotencyPolicy.EXPLICIT;
        }
        if ("CONTEXT_STABLE".equals(normalized) || "RANDOM".equals(normalized)) {
            return IdempotencyPolicy.CONTEXT_STABLE;
        }
        LOG.warning(() -> "Unknown idempotency policy '" + raw
            + "'; falling back to " + IdempotencyPolicy.CONTEXT_STABLE);
        return IdempotencyPolicy.CONTEXT_STABLE;
    }

    /**
     * Returns explicit caller-supplied idempotency key if present.
     *
     * @return explicit key
     */
    public Optional<String> explicitIdempotencyKey() {
        String raw = attributes.get(ATTR_IDEMPOTENCY_KEY);
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(raw.trim());
    }
}
