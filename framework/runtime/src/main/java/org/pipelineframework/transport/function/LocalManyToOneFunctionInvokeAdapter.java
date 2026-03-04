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

import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Local invoke adapter for N->1 function transport flows.
 *
 * @param <I> input payload type
 * @param <O> output payload type
 */
public final class LocalManyToOneFunctionInvokeAdapter<I, O> implements FunctionInvokeAdapter<I, O> {
    private static final Comparator<TraceEnvelope<?>> LINEAGE_ORDER = Comparator
        .comparing((TraceEnvelope<?> envelope) -> AdapterUtils.normalizeOrDefault(envelope.itemId(), ""))
        .thenComparing(envelope -> AdapterUtils.normalizeOrDefault(envelope.idempotencyKey(), ""));

    private final Function<Multi<I>, Uni<O>> delegate;
    private final String outputPayloadModel;
    private final String outputPayloadModelVersion;
    private final BatchingPolicy batchingPolicy;

    /**
     * Creates a local invoke adapter.
     *
     * @param delegate delegate function
     * @param outputPayloadModel output model name
     * @param outputPayloadModelVersion output model version
     */
    public LocalManyToOneFunctionInvokeAdapter(
            Function<Multi<I>, Uni<O>> delegate,
            String outputPayloadModel,
            String outputPayloadModelVersion) {
        this(delegate, outputPayloadModel, outputPayloadModelVersion, BatchingPolicy.defaultPolicy());
    }

    public LocalManyToOneFunctionInvokeAdapter(
            Function<Multi<I>, Uni<O>> delegate,
            String outputPayloadModel,
            String outputPayloadModelVersion,
            BatchingPolicy batchingPolicy) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.outputPayloadModel = AdapterUtils.normalizeOrDefault(outputPayloadModel, "unknown.output");
        this.outputPayloadModelVersion = AdapterUtils.normalizeOrDefault(outputPayloadModelVersion, "v1");
        this.batchingPolicy = Objects.requireNonNull(batchingPolicy, "batchingPolicy must not be null");
    }

    @Override
    public Uni<TraceEnvelope<O>> invokeManyToOne(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input stream must not be null");
        // Context is validated for interface contract consistency; local aggregation currently has no context-specific behavior.
        Objects.requireNonNull(context, "context must not be null");
        return boundedCollect(input).onItem().transformToUni(envelopes -> {
            Multi<I> payloadStream = Multi.createFrom().iterable(envelopes).onItem().transform(TraceEnvelope::payload);
            return delegate.apply(payloadStream)
                .onItem().ifNull().failWith(() -> new NullPointerException(
                    "LocalManyToOneFunctionInvokeAdapter delegate emitted null output"))
                .onItem().transform(output -> createOutputEnvelope(envelopes, output, context));
        });
    }

    private Uni<List<TraceEnvelope<I>>> boundedCollect(Multi<TraceEnvelope<I>> input) {
        int maxItems = batchingPolicy.maxItems();
        return switch (batchingPolicy.overflowPolicy()) {
            case FAIL -> input.select().first(maxItems + 1).collect().asList()
                .onItem().transform(collected -> {
                    if (collected.size() > maxItems) {
                        throw new IllegalStateException(
                            "Function invoke overflow detected with overflowPolicy="
                                + batchingPolicy.overflowPolicy() + ": received more than " + maxItems
                                + " items; collected at least " + collected.size());
                    }
                    return collected;
                });
            case DROP, BUFFER -> input.select().first(maxItems).collect().asList();
        };
    }

    private TraceEnvelope<O> createOutputEnvelope(
            List<TraceEnvelope<I>> envelopes,
            O output,
            FunctionTransportContext context) {
        List<TraceEnvelope<I>> ordered = envelopes.stream()
            .filter(Objects::nonNull)
            .sorted(lineageOrder())
            .toList();
        String traceId = computeTraceId(ordered, context);
        String idempotencyKey = computeIdempotencyKey(ordered, traceId, context);
        TraceLink previous = computePreviousTraceLink(ordered);
        return new TraceEnvelope<>(
            traceId,
            null, // spanId
            computeMergedItemId(ordered, traceId),
            previous,
            outputPayloadModel,
            outputPayloadModelVersion,
            idempotencyKey,
            output,
            null, // occurredAt (generated by TraceEnvelope)
            mergeMeta(ordered, previous));
    }

    private String computeTraceId(List<TraceEnvelope<I>> envelopes, FunctionTransportContext context) {
        if (envelopes.isEmpty()) {
            return AdapterUtils.deriveTraceId(context.requestId());
        }
        String incomingTraceId = envelopes.get(0).traceId();
        if (incomingTraceId == null || incomingTraceId.isBlank()) {
            return AdapterUtils.deriveTraceId(context.requestId());
        }
        return incomingTraceId.strip();
    }

    private String computeIdempotencyKey(
            List<TraceEnvelope<I>> envelopes,
            String traceId,
            FunctionTransportContext context) {
        String deterministicSuffix = deterministicFallbackIdempotencyKey(envelopes);
        return IdempotencyKeyResolver.resolve(context, traceId, outputPayloadModel, deterministicSuffix);
    }

    private TraceLink computePreviousTraceLink(List<TraceEnvelope<I>> envelopes) {
        if (envelopes.isEmpty()) {
            return null;
        }
        String previousItemId = envelopes.get(0).itemId();
        return previousItemId == null ? null : TraceLink.reference(previousItemId);
    }

    private Map<String, String> mergeMeta(List<TraceEnvelope<I>> envelopes, TraceLink previous) {
        LinkedHashMap<String, String> merged = new LinkedHashMap<>();
        for (TraceEnvelope<I> envelope : envelopes) {
            if (envelope != null && envelope.meta() != null && !envelope.meta().isEmpty()) {
                for (Map.Entry<String, String> entry : envelope.meta().entrySet()) {
                    if (entry.getKey() == null) {
                        continue;
                    }
                    String incoming = AdapterUtils.normalizeOrDefault(entry.getValue(), "");
                    merged.merge(entry.getKey(), incoming, this::mergeMetaValues);
                }
            }
        }
        String previousIds = envelopes.stream()
            .map(TraceEnvelope::itemId)
            .filter(Objects::nonNull)
            .map(this::escapeCommaDelimitedComponent)
            .collect(Collectors.joining(","));
        if (!previousIds.isEmpty()) {
            merged.put("previousItemIds", previousIds);
        }
        if (previous != null && previous.previousItemId() != null) {
            merged.putIfAbsent("previousItemId", previous.previousItemId());
        }
        return Map.copyOf(merged);
    }

    private String mergeMetaValues(String existing, String incoming) {
        String normalizedExisting = AdapterUtils.normalizeOrDefault(existing, "");
        String normalizedIncoming = AdapterUtils.normalizeOrDefault(incoming, "");
        if (normalizedExisting.isEmpty()) {
            return normalizedIncoming;
        }
        if (normalizedIncoming.isEmpty()) {
            return normalizedExisting;
        }
        return normalizedExisting + "," + normalizedIncoming;
    }

    private String deterministicFallbackIdempotencyKey(List<TraceEnvelope<I>> envelopes) {
        if (envelopes.isEmpty()) {
            return "empty";
        }
        List<String> components = envelopes.stream()
            .map(envelope -> AdapterUtils.normalizeOrDefault(
                envelope.idempotencyKey(),
                AdapterUtils.normalizeOrDefault(envelope.itemId(), "item")))
            .map(this::escapeDelimitedComponent)
            .toList();
        return String.join("|", components);
    }

    private String escapeDelimitedComponent(String component) {
        return component.replace("\\", "\\\\").replace("|", "\\|");
    }

    private String escapeCommaDelimitedComponent(String component) {
        return component.replace("\\", "\\\\").replace(",", "\\,");
    }

    private String computeMergedItemId(List<TraceEnvelope<I>> ordered, String traceId) {
        if (ordered.isEmpty()) {
            return AdapterUtils.deterministicId(
                "invoke-many-to-one-empty",
                traceId,
                outputPayloadModel,
                outputPayloadModelVersion);
        }
        StringBuilder lineageDigestBuilder = new StringBuilder();
        for (TraceEnvelope<I> envelope : ordered) {
            String normalizedItemId = AdapterUtils.normalizeOrDefault(envelope.itemId(), "item");
            lineageDigestBuilder
                .append('#')
                .append(normalizedItemId.length())
                .append(':')
                .append(normalizedItemId);
        }
        String lineageDigest = lineageDigestBuilder.toString();
        return AdapterUtils.deterministicId(
            "invoke-many-to-one",
            traceId,
            outputPayloadModel,
            outputPayloadModelVersion,
            lineageDigest);
    }

    private Comparator<TraceEnvelope<I>> lineageOrder() {
        // Merge lineage is deterministic: inputs are ordered by stable envelope attributes.
        // This ordering drives previousItemRef, previousItemIds metadata, merged itemId, and idempotency key.
        return LocalManyToOneFunctionInvokeAdapter.<TraceEnvelope<I>>asTypedComparator(LINEAGE_ORDER)
            .thenComparing(envelope -> AdapterUtils.normalizeOrDefault(envelope.traceId(), ""))
            .thenComparing(envelope -> AdapterUtils.normalizeOrDefault(envelope.payloadModel(), ""))
            .thenComparing(envelope -> AdapterUtils.normalizeOrDefault(envelope.payloadModelVersion(), ""))
            .thenComparing(envelope -> envelope.previousItemRef() == null
                ? ""
                : AdapterUtils.normalizeOrDefault(envelope.previousItemRef().previousItemId(), ""))
            .thenComparing(this::metaFingerprint)
            .thenComparing(this::payloadFingerprint);
    }

    private String metaFingerprint(TraceEnvelope<I> envelope) {
        if (envelope.meta() == null || envelope.meta().isEmpty()) {
            return "";
        }
        return envelope.meta().entrySet().stream()
            .sorted(Comparator
                .comparing((Map.Entry<String, String> entry) -> AdapterUtils.normalizeOrDefault(entry.getKey(), ""))
                .thenComparing(entry -> AdapterUtils.normalizeOrDefault(entry.getValue(), "")))
            .map(entry -> AdapterUtils.normalizeOrDefault(entry.getKey(), "")
                + "=" + AdapterUtils.normalizeOrDefault(entry.getValue(), ""))
            .collect(Collectors.joining(";"));
    }

    /**
     * Produce a deterministic fingerprint for the envelope's payload.
     *
     * @param envelope the trace envelope whose payload will be fingerprinted
     * @return `ClassName:SHA-256_HEX` for a non-null payload, or an empty string if the payload is null
     */
    private String payloadFingerprint(TraceEnvelope<I> envelope) {
        Object payload = envelope.payload();
        if (payload == null) {
            return "";
        }
        String canonical = canonicalPayload(payload, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
        return payload.getClass().getName() + ":" + sha256Hex(canonical);
    }

    /**
     * Produce a deterministic canonical string representation of a payload for stable fingerprinting.
     *
     * <p>Supports the following forms:
     * <ul>
     *   <li>null -> "null"</li>
     *   <li>CharSequence, Number, Boolean, Enum -> their toString()</li>
     *   <li>Map -> sorted entries formatted as "{key->value,...}" where keys and values are canonicalized</li>
     *   <li>Iterable and arrays -> elements formatted as "[e1,e2,...]" with canonicalized elements</li>
     *   <li>Record -> "ClassName{field=value,...}" with recursive canonicalization and cycle detection (returns "ClassName#cycle" for a detected cycle)</li>
     *   <li>Other objects -> "ClassName:normalizedToString" unless toString() looks like "ClassName@hex", in which case "ClassName" is returned</li>
     * </ul>
     *
     * @param payload the value to canonicalize
     * @param visited a mutable set used to track already-visited reference instances to detect cycles (identity semantics expected)
     * @return a stable, deterministic string representation of the payload suitable for hashing and comparison
     */
    private String canonicalPayload(Object payload, Set<Object> visited) {
        if (payload == null) {
            return "null";
        }
        if (payload instanceof CharSequence
                || payload instanceof Number
                || payload instanceof Boolean
                || payload instanceof Enum<?>) {
            return payload.toString();
        }
        if (payload instanceof Map<?, ?> map) {
            return map.entrySet().stream()
                .map(entry -> canonicalPayload(entry.getKey(), visited)
                    + "->" + canonicalPayload(entry.getValue(), visited))
                .sorted()
                .collect(Collectors.joining(",", "{", "}"));
        }
        if (payload instanceof Iterable<?> iterable) {
            StringBuilder builder = new StringBuilder("[");
            boolean first = true;
            for (Object item : iterable) {
                if (!first) {
                    builder.append(',');
                }
                builder.append(canonicalPayload(item, visited));
                first = false;
            }
            return builder.append(']').toString();
        }
        Class<?> payloadClass = payload.getClass();
        if (payloadClass.isArray()) {
            int length = Array.getLength(payload);
            StringBuilder builder = new StringBuilder("[");
            for (int i = 0; i < length; i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append(canonicalPayload(Array.get(payload, i), visited));
            }
            return builder.append(']').toString();
        }
        if (payloadClass.isRecord()) {
            if (!visited.add(payload)) {
                return payloadClass.getName() + "#cycle";
            }
            try {
                StringBuilder builder = new StringBuilder(payloadClass.getName()).append('{');
                RecordComponent[] components = payloadClass.getRecordComponents();
                for (int i = 0; i < components.length; i++) {
                    if (i > 0) {
                        builder.append(',');
                    }
                    RecordComponent component = components[i];
                    Object value = component.getAccessor().invoke(payload);
                    builder.append(component.getName()).append('=').append(canonicalPayload(value, visited));
                }
                return builder.append('}').toString();
            } catch (ReflectiveOperationException ignored) {
                return payloadClass.getName() + ":" + AdapterUtils.normalizeOrDefault(payload.toString(), "");
            } finally {
                visited.remove(payload);
            }
        }
        String rendered = AdapterUtils.normalizeOrDefault(payload.toString(), "");
        String identityPrefix = payloadClass.getName() + "@";
        if (rendered.startsWith(identityPrefix)) {
            String suffix = rendered.substring(identityPrefix.length());
            if (!suffix.isEmpty() && suffix.chars().allMatch(ch ->
                (ch >= '0' && ch <= '9')
                    || (ch >= 'a' && ch <= 'f')
                    || (ch >= 'A' && ch <= 'F'))) {
                return payloadClass.getName();
            }
        }
        return payloadClass.getName() + ":" + rendered;
    }

    /**
     * Compute the SHA-256 digest of the given string and return it as a lowercase hexadecimal string.
     *
     * @param value the input string to hash (treated as UTF-8)
     * @return the SHA-256 digest encoded as a lowercase hex string
     * @throws IllegalStateException if the SHA-256 algorithm is not available on the platform
     */
    private String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(Character.forDigit((b >>> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm is not available", e);
        }
    }

    /**
     * Casts a comparator with wildcard bounds to a comparator typed to `T`.
     *
     * <p>This is a convenience helper to treat a `Comparator<? super T>` as a `Comparator<T>` when such
     * a cast is known to be safe at the call site.
     *
     * @param comparator the comparator to cast
     * @return the provided comparator cast to `Comparator<T>`
     */
    @SuppressWarnings("unchecked")
    private static <T> Comparator<T> asTypedComparator(Comparator<? super T> comparator) {
        return (Comparator<T>) comparator;
    }
}
