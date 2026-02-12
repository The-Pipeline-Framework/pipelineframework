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

/**
 * Executes a unary function transport pipeline: source -> invoke -> sink.
 */
public final class UnaryFunctionTransportBridge {
    private UnaryFunctionTransportBridge() {
    }

    /**
     * Executes unary adaptation, invocation, and egress synchronously for Lambda handlers.
     *
     * @param event inbound event payload
     * @param context function transport context
     * @param sourceAdapter source adapter
     * @param invokeAdapter invoke adapter
     * @param sinkAdapter sink adapter
     * @param <I> input payload type
     * @param <O> output payload type
     * @return final sink payload
     * @throws NullPointerException when any required parameter is null; delegates to
     *     {@link FunctionTransportBridge#invokeOneToOne(Object, FunctionTransportContext, FunctionSourceAdapter, FunctionInvokeAdapter, FunctionSinkAdapter)}
     *     after local null validation
     */
    public static <I, O> O invoke(
            I event,
            FunctionTransportContext context,
            FunctionSourceAdapter<I, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, O> sinkAdapter) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(sourceAdapter, "sourceAdapter must not be null");
        Objects.requireNonNull(invokeAdapter, "invokeAdapter must not be null");
        Objects.requireNonNull(sinkAdapter, "sinkAdapter must not be null");
        return FunctionTransportBridge.invokeOneToOne(event, context, sourceAdapter, invokeAdapter, sinkAdapter);
    }
}
