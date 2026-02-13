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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Routes function invocation calls to local or remote adapter delegates based on {@link FunctionInvocationMode}.
 *
 * @param <I> input payload type
 * @param <O> output payload type
 */
public final class InvocationModeRoutingFunctionInvokeAdapter<I, O> implements FunctionInvokeAdapter<I, O> {
    private final FunctionInvokeAdapter<I, O> localDelegate;
    private final FunctionInvokeAdapter<I, O> remoteDelegate;

    /**
     * Creates a routing adapter.
     *
     * @param localDelegate local invocation delegate
     * @param remoteDelegate remote invocation delegate
     */
    public InvocationModeRoutingFunctionInvokeAdapter(
            FunctionInvokeAdapter<I, O> localDelegate,
            FunctionInvokeAdapter<I, O> remoteDelegate) {
        this.localDelegate = Objects.requireNonNull(localDelegate, "localDelegate must not be null");
        this.remoteDelegate = Objects.requireNonNull(remoteDelegate, "remoteDelegate must not be null");
    }

    @Override
    public Uni<TraceEnvelope<O>> invokeOneToOne(TraceEnvelope<I> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input envelope must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return selectDelegate(context).invokeOneToOne(input, context);
    }

    @Override
    public Multi<TraceEnvelope<O>> invokeOneToMany(TraceEnvelope<I> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input envelope must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return selectDelegate(context).invokeOneToMany(input, context);
    }

    @Override
    public Uni<TraceEnvelope<O>> invokeManyToOne(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input stream must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return selectDelegate(context).invokeManyToOne(input, context);
    }

    @Override
    public Multi<TraceEnvelope<O>> invokeManyToMany(Multi<TraceEnvelope<I>> input, FunctionTransportContext context) {
        Objects.requireNonNull(input, "input stream must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return selectDelegate(context).invokeManyToMany(input, context);
    }

    private FunctionInvokeAdapter<I, O> selectDelegate(FunctionTransportContext context) {
        return context.invocationMode() == FunctionInvocationMode.REMOTE ? remoteDelegate : localDelegate;
    }
}

