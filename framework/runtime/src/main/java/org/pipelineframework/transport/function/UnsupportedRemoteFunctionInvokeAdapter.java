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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Remote invocation delegate that fails fast until a concrete remote adapter is configured.
 *
 * @param <I> input payload type
 * @param <O> output payload type
 */
public final class UnsupportedRemoteFunctionInvokeAdapter<I, O> implements FunctionInvokeAdapter<I, O> {

    @Override
    public Uni<TraceEnvelope<O>> invokeOneToOne(TraceEnvelope<I> input, FunctionTransportContext context) {
        return Uni.createFrom().failure(new UnsupportedOperationException(buildMessage(context)));
    }

    @Override
    public Multi<TraceEnvelope<O>> invokeOneToMany(TraceEnvelope<I> input, FunctionTransportContext context) {
        return Multi.createFrom().failure(new UnsupportedOperationException(buildMessage(context)));
    }

    @Override
    public Uni<TraceEnvelope<O>> invokeManyToOne(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        return Uni.createFrom().failure(new UnsupportedOperationException(buildMessage(context)));
    }

    @Override
    public Multi<TraceEnvelope<O>> invokeManyToMany(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        return Multi.createFrom().failure(new UnsupportedOperationException(buildMessage(context)));
    }

    private String buildMessage(FunctionTransportContext context) {
        String target = "runtime=" + context.targetRuntime().orElse("n/a")
            + ", module=" + context.targetModule().orElse("n/a")
            + ", handler=" + context.targetHandler().orElse("n/a");
        return "Function invocation mode is REMOTE but no remote invoke adapter is configured (" + target + ").";
    }
}

