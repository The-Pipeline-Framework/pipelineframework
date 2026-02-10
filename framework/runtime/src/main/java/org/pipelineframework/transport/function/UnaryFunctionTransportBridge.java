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
import java.util.Objects;

import io.smallrye.mutiny.Uni;

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
     */
    public static <I, O> O invoke(
            I event,
            FunctionTransportContext context,
            FunctionSourceAdapter<I, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, O> sinkAdapter) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(sourceAdapter, "sourceAdapter must not be null");
        Objects.requireNonNull(invokeAdapter, "invokeAdapter must not be null");
        Objects.requireNonNull(sinkAdapter, "sinkAdapter must not be null");

        Uni<TraceEnvelope<I>> inbound = sourceAdapter.adapt(event, context)
            .collect().asList()
            .onItem().transform(items -> requireExactlyOne(items, "source"));

        Uni<TraceEnvelope<O>> outbound = inbound.onItem()
            .transformToUni(envelope -> invokeAdapter.invokeOneToOne(envelope, context));

        return outbound.onItem()
            .transformToUni(envelope -> sinkAdapter.emitOne(envelope, context))
            .await().indefinitely();
    }

    private static <T> T requireExactlyOne(List<T> items, String stage) {
        if (items == null || items.isEmpty()) {
            throw new IllegalStateException(
                "Unary function transport expected exactly one " + stage + " item but received none.");
        }
        if (items.size() > 1) {
            throw new IllegalStateException(
                "Unary function transport expected exactly one " + stage + " item but received " + items.size() + ".");
        }
        return items.get(0);
    }
}
