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

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private String computeMergedItemId(List<TraceEnvelope<I>> ordered, String traceId) {
        if (ordered.isEmpty()) {
            return AdapterUtils.deterministicId(
                "invoke-many-to-one-empty",
                traceId,
                outputPayloadModel,
                outputPayloadModelVersion);
        }
        String lineageDigest = ordered.stream()
            .map(envelope -> AdapterUtils.normalizeOrDefault(envelope.itemId(), "item"))
            .collect(Collectors.joining("|"));
        return AdapterUtils.deterministicId(
            "invoke-many-to-one",
            traceId,
            outputPayloadModel,
            outputPayloadModelVersion,
            lineageDigest);
    }

    private Comparator<TraceEnvelope<I>> lineageOrder() {
        // Merge lineage is deterministic: inputs are ordered by itemId then idempotencyKey.
        // This ordering drives previousItemRef, previousItemIds metadata, merged itemId, and idempotency key.
        return typedComparator(LINEAGE_ORDER);
    }

    @SuppressWarnings("unchecked")
    private static <T> Comparator<T> typedComparator(Comparator<?> comparator) {
        return (Comparator<T>) comparator;
    }
}
