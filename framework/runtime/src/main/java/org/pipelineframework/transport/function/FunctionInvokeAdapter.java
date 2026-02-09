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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Abstraction for orchestrator-to-step invocation across function-runtime boundaries.
 *
 * <p>Implementations can map each shape to direct invocation, queue/topic handoff, or streaming bridge,
 * while keeping {@code Multi}/{@code Uni} contracts inside the pipeline runtime.</p>
 *
 * @param <I> input payload type
 * @param <O> output payload type
 */
public interface FunctionInvokeAdapter<I, O> {

    /**
     * 1 -> 1 invocation.
     *
     * @param input input envelope
     * @param context invocation context
     * @return unary response
     */
    default Uni<TraceEnvelope<O>> invokeOneToOne(TraceEnvelope<I> input, FunctionTransportContext context) {
        return Uni.createFrom().failure(new UnsupportedOperationException("1->1 invocation is not implemented"));
    }

    /**
     * 1 -> N invocation.
     *
     * @param input input envelope
     * @param context invocation context
     * @return streamed response
     */
    default Multi<TraceEnvelope<O>> invokeOneToMany(TraceEnvelope<I> input, FunctionTransportContext context) {
        return Multi.createFrom().failure(new UnsupportedOperationException("1->N invocation is not implemented"));
    }

    /**
     * N -> 1 invocation.
     *
     * @param input input stream
     * @param context invocation context
     * @return unary response
     */
    default Uni<TraceEnvelope<O>> invokeManyToOne(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        return Uni.createFrom().failure(new UnsupportedOperationException("N->1 invocation is not implemented"));
    }

    /**
     * N -> M invocation.
     *
     * @param input input stream
     * @param context invocation context
     * @return streamed response
     */
    default Multi<TraceEnvelope<O>> invokeManyToMany(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        return Multi.createFrom().failure(new UnsupportedOperationException("N->M invocation is not implemented"));
    }
}

