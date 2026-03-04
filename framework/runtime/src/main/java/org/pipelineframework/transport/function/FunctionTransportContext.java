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
    public static final String ATTR_INVOCATION_MODE = "tpf.function.invocation.mode";
    public static final String ATTR_TARGET_URL = "tpf.function.target.url";
    public static final String ATTR_TARGET_RUNTIME = "tpf.function.target.runtime";
    public static final String ATTR_TARGET_MODULE = "tpf.function.target.module";
    public static final String ATTR_TARGET_HANDLER = "tpf.function.target.handler";
    public static final String ATTR_TRANSPORT_PROTOCOL = "tpf.transport.protocol";
    public static final String ATTR_CORRELATION_ID = "tpf.correlation.id";
    public static final String ATTR_EXECUTION_ID = "tpf.execution.id";
    public static final String ATTR_RETRY_ATTEMPT = "tpf.retry.attempt";
    public static final String ATTR_DEADLINE_EPOCH_MS = "tpf.deadline.epoch.ms";
    public static final String ATTR_DISPATCH_TS_EPOCH_MS = "tpf.dispatch.ts.epoch.ms";
    public static final String ATTR_PARENT_ITEM_ID = "tpf.parent.item.id";

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
     * Creates a context with attributes.
     *
     * @param requestId request id
     * @param functionName function name
     * @param stage stage name
     * @param attributes context attributes
     * @return context with attributes
     */
    public static FunctionTransportContext of(
            String requestId,
            String functionName,
            String stage,
            Map<String, String> attributes) {
        return new FunctionTransportContext(requestId, functionName, stage, attributes);
    }

    /**
     * Resolves idempotency policy from context attributes.
     *
     * @return resolved policy; defaults to CONTEXT_STABLE when unset.
     *     Legacy RANDOM is accepted as compatibility alias and normalized to CONTEXT_STABLE.
     * @throws IllegalArgumentException when a non-empty idempotency policy is not supported
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
        throw new IllegalArgumentException(
            "Unsupported idempotency policy '" + raw + "'. Expected CONTEXT_STABLE, RANDOM, or EXPLICIT.");
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
        return Optional.of(raw.strip());
    }

    /**
     * Resolves function invocation mode from context attributes.
     *
     * @return invocation mode; defaults to LOCAL when unset
     * @throws IllegalArgumentException when a non-empty invocation mode value is not LOCAL or REMOTE
     */
    public FunctionInvocationMode invocationMode() {
        String raw = attributes.get(ATTR_INVOCATION_MODE);
        if (raw == null || raw.isBlank()) {
            return FunctionInvocationMode.LOCAL;
        }
        String normalized = raw.strip().toUpperCase(Locale.ROOT);
        if ("LOCAL".equals(normalized)) {
            return FunctionInvocationMode.LOCAL;
        }
        if ("REMOTE".equals(normalized)) {
            return FunctionInvocationMode.REMOTE;
        }
        throw new IllegalArgumentException(
            "Unsupported function invocation mode '" + raw + "'. Expected LOCAL or REMOTE.");
    }

    /**
     * Optional target runtime identifier for remote invocation.
     * Only meaningful when {@link #invocationMode()} is {@link FunctionInvocationMode#REMOTE}.
     *
     * @return target runtime when present
     */
    public Optional<String> targetRuntime() {
        return normalizedAttribute(ATTR_TARGET_RUNTIME);
    }

    /**
     * Optional target URL for remote invocation transport.
     * Only meaningful when {@link #invocationMode()} is {@link FunctionInvocationMode#REMOTE}.
     *
     * @return target URL when present
     */
    public Optional<String> targetUrl() {
        return normalizedAttribute(ATTR_TARGET_URL);
    }

    /**
     * Optional target module identifier for remote invocation.
     * Only meaningful when {@link #invocationMode()} is {@link FunctionInvocationMode#REMOTE}.
     *
     * @return target module when present
     */
    public Optional<String> targetModule() {
        return normalizedAttribute(ATTR_TARGET_MODULE);
    }

    /**
     * Optional target handler identifier for remote invocation.
     * Only meaningful when {@link #invocationMode()} is {@link FunctionInvocationMode#REMOTE}.
     *
     * @return target handler when present
     */
    public Optional<String> targetHandler() {
        return normalizedAttribute(ATTR_TARGET_HANDLER);
    }

    /**
     * Obtain the trimmed attribute value for the given key when it exists and is not blank.
     *
     * @param key the attribute key to look up
     * @return an {@link Optional} containing the attribute value with surrounding whitespace removed if present and not blank, otherwise {@link Optional#empty()}
     */
    private Optional<String> normalizedAttribute(String key) {
        String raw = attributes.get(key);
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(raw.strip());
    }

    /**
     * Configured transport protocol identifier when available.
     *
     * @return an Optional containing the normalized protocol identifier if present, otherwise empty
     */
    public Optional<String> transportProtocol() {
        return normalizedAttribute(ATTR_TRANSPORT_PROTOCOL);
    }

    /**
     * Retrieve the correlation identifier if present.
     *
     * @return an Optional containing the correlation id if present, otherwise an empty Optional
     */
    public Optional<String> correlationId() {
        return normalizedAttribute(ATTR_CORRELATION_ID);
    }

    /**
     * Retrieves the execution identifier from the context attributes.
     *
     * @return an {@code Optional} containing the execution id if present and non-blank, otherwise an empty {@code Optional}
     */
    public Optional<String> executionId() {
        return normalizedAttribute(ATTR_EXECUTION_ID);
    }

    /**
     * Retrieve the retry attempt count from the context attributes when available.
     *
     * @return an Optional containing the retry attempt count as an Integer if the attribute is present and a valid integer, otherwise an empty Optional
     */
    public Optional<Integer> retryAttempt() {
        return normalizedAttribute(ATTR_RETRY_ATTEMPT)
            .flatMap(value -> toInteger(ATTR_RETRY_ATTEMPT, value));
    }

    /**
     * Get the absolute deadline timestamp in epoch milliseconds when present.
     *
     * <p>Parses the ATTR_DEADLINE_EPOCH_MS attribute as a long; if the attribute is missing or
     * cannot be parsed as a long, an empty Optional is returned.
     *
     * @return an {@link Optional} containing the deadline timestamp in milliseconds when present and parsable; empty otherwise
     */
    public Optional<Long> deadlineEpochMs() {
        return normalizedAttribute(ATTR_DEADLINE_EPOCH_MS)
            .flatMap(value -> toLong(ATTR_DEADLINE_EPOCH_MS, value));
    }

    /**
     * Dispatch timestamp in epoch milliseconds if present and parseable.
     *
     * @return an Optional containing the dispatch timestamp (milliseconds since epoch), or empty if the attribute is missing or cannot be parsed as a long
     */
    public Optional<Long> dispatchTsEpochMs() {
        return normalizedAttribute(ATTR_DISPATCH_TS_EPOCH_MS)
            .flatMap(value -> toLong(ATTR_DISPATCH_TS_EPOCH_MS, value));
    }

    /**
     * Retrieves the lineage parent item identifier if present.
     *
     * @return an Optional containing the parent item id if present, otherwise an empty Optional
     */
    public Optional<String> parentItemId() {
        return normalizedAttribute(ATTR_PARENT_ITEM_ID);
    }

    /**
     * Parses a string attribute value as an integer, returning an absent optional on parse failure.
     *
     * @param key   the attribute key (used only for logging on parse failure)
     * @param value the string value to parse as an integer
     * @return      an Optional containing the parsed integer if parsing succeeds, otherwise Optional.empty()
     */
    private Optional<Integer> toInteger(String key, String value) {
        try {
            return Optional.of(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            LOG.warning(() -> "Ignoring invalid integer attribute '" + key + "' value '" + value + "'");
            return Optional.empty();
        }
    }

    /**
     * Parses the provided attribute string value into a `Long` for the given attribute key.
     *
     * @param key   the attribute key (used in the warning message if parsing fails)
     * @param value the string value to parse as a long
     * @return      an Optional containing the parsed `Long`, or `Optional.empty()` if the value cannot be parsed;
     *              a warning is logged when parsing fails
     */
    private Optional<Long> toLong(String key, String value) {
        try {
            return Optional.of(Long.parseLong(value));
        } catch (NumberFormatException e) {
            LOG.warning(() -> "Ignoring invalid long attribute '" + key + "' value '" + value + "'");
            return Optional.empty();
        }
    }
}
