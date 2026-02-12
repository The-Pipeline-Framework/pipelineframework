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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import io.smallrye.mutiny.Multi;

/**
 * Local invoke adapter for 1->N function transport flows.
 *
 * @param <I> input payload type
 * @param <O> output payload type
 */
public final class LocalOneToManyFunctionInvokeAdapter<I, O> implements FunctionInvokeAdapter<I, O> {
    private final Function<I, Multi<O>> delegate;
    private final String outputPayloadModel;
    private final String outputPayloadModelVersion;

    /**
     * Creates a local invoke adapter.
     *
     * @param delegate delegate function
     * @param outputPayloadModel output model name
     * @param outputPayloadModelVersion output model version
     */
    public LocalOneToManyFunctionInvokeAdapter(
            Function<I, Multi<O>> delegate,
            String outputPayloadModel,
            String outputPayloadModelVersion) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.outputPayloadModel = AdapterUtils.normalizeOrDefault(outputPayloadModel, "unknown.output");
        this.outputPayloadModelVersion = AdapterUtils.normalizeOrDefault(outputPayloadModelVersion, "v1");
    }

    @Override
    public Multi<TraceEnvelope<O>> invokeOneToMany(TraceEnvelope<I> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input envelope must not be null");
        // Context is required by the FunctionInvokeAdapter contract but not used for local 1->N invocation.
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(input.payload(), "LocalUnaryFunctionInvokeAdapter input payload must not be null");
        String traceId = AdapterUtils.normalizeOrDefault(input.traceId(), AdapterUtils.deriveTraceId(context.requestId()));
        AtomicLong outputIndex = new AtomicLong(0L);
        return delegate.apply(input.payload())
            .onItem().transform(output -> {
                if (output == null) {
                    throw new NullPointerException("LocalOneToManyFunctionInvokeAdapter delegate emitted null output");
                }
                String envelopeId = UUID.randomUUID().toString();
                return input.next(
                    envelopeId,
                    outputPayloadModel,
                    outputPayloadModelVersion,
                    resolveIdempotencyKey(context, traceId, input, outputIndex.getAndIncrement()),
                    output);
            });
    }

    private String resolveIdempotencyKey(
            FunctionTransportContext context,
            String traceId,
            TraceEnvelope<I> input,
            long outputIndex) {
        String inherited = AdapterUtils.normalizeOrDefault(input.idempotencyKey(), "");
        String suffix = !inherited.isEmpty()
            ? inherited + ":" + outputIndex
            : Long.toString(outputIndex);
        return IdempotencyKeyResolver.resolve(context, traceId, outputPayloadModel, suffix, Long.toString(outputIndex));
    }

}
