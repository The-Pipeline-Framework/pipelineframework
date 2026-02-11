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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
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
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.outputPayloadModel = normalizeOrDefault(outputPayloadModel, "unknown.output");
        this.outputPayloadModelVersion = normalizeOrDefault(outputPayloadModelVersion, "v1");
    }

    @Override
    public Uni<TraceEnvelope<O>> invokeManyToOne(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input stream must not be null");
        return input.collect().asList().onItem().transformToUni(envelopes -> {
            Multi<I> payloadStream = Multi.createFrom().iterable(envelopes).onItem().transform(TraceEnvelope::payload);
            return delegate.apply(payloadStream)
                .onItem().ifNull().failWith(() -> new NullPointerException(
                    "LocalManyToOneFunctionInvokeAdapter delegate emitted null output"))
                .onItem().transform(output -> createOutputEnvelope(envelopes, output));
        });
    }

    private TraceEnvelope<O> createOutputEnvelope(List<TraceEnvelope<I>> envelopes, O output) {
        String traceId = envelopes.isEmpty() ? UUID.randomUUID().toString() : normalizeOrDefault(envelopes.get(0).traceId(), UUID.randomUUID().toString());
        String idempotencyKey = envelopes.isEmpty()
            ? traceId + ":" + outputPayloadModel + ":" + UUID.randomUUID()
            : normalizeOrDefault(envelopes.get(0).idempotencyKey(), traceId + ":" + outputPayloadModel + ":" + UUID.randomUUID());
        String previousItemId = envelopes.isEmpty() ? null : envelopes.get(envelopes.size() - 1).itemId();
        TraceLink previous = previousItemId == null ? null : TraceLink.reference(previousItemId);
        return new TraceEnvelope<>(
            traceId,
            null,
            UUID.randomUUID().toString(),
            previous,
            outputPayloadModel,
            outputPayloadModelVersion,
            idempotencyKey,
            output,
            null,
            Map.of());
    }

    private static String normalizeOrDefault(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }
}
