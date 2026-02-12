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
import java.util.function.Function;

import io.smallrye.mutiny.Uni;

/**
 * Unary invoke adapter that executes a local asynchronous function and wraps the result in a trace envelope.
 *
 * @param <I> input payload type
 * @param <O> output payload type
 */
public final class LocalUnaryFunctionInvokeAdapter<I, O> implements FunctionInvokeAdapter<I, O> {
    private final Function<I, Uni<O>> delegate;
    private final String outputPayloadModel;
    private final String outputPayloadModelVersion;

    /**
     * Creates an invoke adapter.
     *
     * @param delegate local invocation function
     * @param outputPayloadModel output payload model id
     * @param outputPayloadModelVersion output payload model version
     */
    public LocalUnaryFunctionInvokeAdapter(
            Function<I, Uni<O>> delegate,
            String outputPayloadModel,
            String outputPayloadModelVersion) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.outputPayloadModel = AdapterUtils.normalizeOrDefault(outputPayloadModel, "unknown.output");
        this.outputPayloadModelVersion = AdapterUtils.normalizeOrDefault(outputPayloadModelVersion, "v1");
    }

    @Override
    public Uni<TraceEnvelope<O>> invokeOneToOne(TraceEnvelope<I> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input envelope must not be null");
        Objects.requireNonNull(context, "context must not be null");
        I payload = input.payload();
        if (payload == null) {
            return Uni.createFrom().failure(new NullPointerException(
                "LocalUnaryFunctionInvokeAdapter input payload must not be null"));
        }
        return Uni.createFrom().deferred(() -> {
                Uni<O> result = delegate.apply(payload);
                if (result == null) {
                    return Uni.createFrom().failure(new NullPointerException(
                        "LocalUnaryFunctionInvokeAdapter delegate returned null Uni from apply"));
                }
                return result;
            })
            .onItem().ifNull().failWith(() -> new NullPointerException(
                "LocalUnaryFunctionInvokeAdapter delegate emitted null output"))
            .onItem()
            .transform(output -> input.next(
                UUID.randomUUID().toString(),
                outputPayloadModel,
                outputPayloadModelVersion,
                input.idempotencyKey(),
                output));
    }

}
