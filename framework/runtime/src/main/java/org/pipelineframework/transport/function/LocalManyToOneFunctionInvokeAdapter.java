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

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.function.Function;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Local invoke adapter for N->1 function transport flows.
 *
 * @param <I> input payload type
 * @param <O> output payload type
 */
public final class LocalManyToOneFunctionInvokeAdapter<I, O> implements FunctionInvokeAdapter<I, O> {
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
        String traceId = computeTraceId(envelopes);
        String idempotencyKey = computeIdempotencyKey(envelopes, traceId, context);
        TraceLink previous = computePreviousTraceLink(envelopes);
        return new TraceEnvelope<>(
            traceId,
            null, // spanId
            UUID.randomUUID().toString(),
            previous,
            outputPayloadModel,
            outputPayloadModelVersion,
            idempotencyKey,
            output,
            null, // occurredAt (generated by TraceEnvelope)
            mergeMeta(envelopes, previous));
    }

    private String computeTraceId(List<TraceEnvelope<I>> envelopes) {
        if (envelopes.isEmpty()) {
            return UUID.randomUUID().toString();
        }
        String incomingTraceId = envelopes.get(0).traceId();
        if (incomingTraceId == null || incomingTraceId.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return incomingTraceId.trim();
    }

    private String computeIdempotencyKey(
            List<TraceEnvelope<I>> envelopes,
            String traceId,
            FunctionTransportContext context) {
        String deterministicSuffix = deterministicFallbackIdempotencyKey(envelopes, traceId);
        return IdempotencyKeyResolver.resolve(context, traceId, outputPayloadModel, deterministicSuffix);
    }

    private TraceLink computePreviousTraceLink(List<TraceEnvelope<I>> envelopes) {
        if (envelopes.isEmpty()) {
            return null;
        }
        String previousItemId = envelopes.get(envelopes.size() - 1).itemId();
        return previousItemId == null ? null : TraceLink.reference(previousItemId);
    }

    private Map<String, String> mergeMeta(List<TraceEnvelope<I>> envelopes, TraceLink previous) {
        LinkedHashMap<String, String> merged = new LinkedHashMap<>();
        for (TraceEnvelope<I> envelope : envelopes) {
            if (envelope != null && envelope.meta() != null && !envelope.meta().isEmpty()) {
                merged.putAll(envelope.meta());
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

    private String deterministicFallbackIdempotencyKey(List<TraceEnvelope<I>> envelopes, String traceId) {
        if (envelopes.isEmpty()) {
            return "empty";
        }
        TraceEnvelope<I> first = envelopes.get(0);
        String stable = AdapterUtils.normalizeOrDefault(first.idempotencyKey(),
            AdapterUtils.normalizeOrDefault(first.itemId(), "item0"));
        return stable;
    }
}
