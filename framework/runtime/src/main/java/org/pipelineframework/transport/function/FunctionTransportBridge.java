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

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Executes function transport flows for unary and streaming cardinalities.
 *
 * <p>All invoke methods in this bridge block the caller by using {@code await().atMost(timeout)}.
 * Calls may block for up to {@link #DEFAULT_WAIT_TIMEOUT} (or a supplied timeout), so callers in
 * reactive/non-blocking contexts should offload execution to worker threads or use non-blocking
 * alternatives at a higher layer. Reactive companion methods are provided to avoid blocking.</p>
 */
public final class FunctionTransportBridge {
    private static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(30);

    private FunctionTransportBridge() {
    }

    /**
     * Executes a 1 -> 1 flow.
     */
    public static <E, I, O, R> R invokeOneToOne(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        return invokeOneToOne(event, context, sourceAdapter, invokeAdapter, sinkAdapter, DEFAULT_WAIT_TIMEOUT);
    }

    /**
     * Executes a 1 -> 1 flow with explicit timeout.
     */
    public static <E, I, O, R> R invokeOneToOne(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter,
            Duration timeout) {
        validateTimeout(timeout);
        return invokeOneToOneReactive(event, context, sourceAdapter, invokeAdapter, sinkAdapter)
            .await().atMost(timeout);
    }

    /**
     * Executes a non-blocking reactive 1 -> 1 flow and returns a {@link Uni} for the sink result.
     *
     * @param event inbound event payload
     * @param context function transport context
     * @param sourceAdapter source adapter
     * @param invokeAdapter invoke adapter
     * @param sinkAdapter sink adapter
     * @param <E> event type
     * @param <I> inbound payload type
     * @param <O> outbound payload type
     * @param <R> sink result type
     * @return reactive sink result
     */
    public static <E, I, O, R> Uni<R> invokeOneToOneReactive(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        validateParameters(event, context, sourceAdapter, invokeAdapter, sinkAdapter);

        Uni<TraceEnvelope<I>> inbound = requireSingleSource(event, context, sourceAdapter);

        Uni<TraceEnvelope<O>> outbound = inbound.onItem()
            .transformToUni(envelope -> invokeAdapter.invokeOneToOne(envelope, context));

        return outbound.onItem()
            .transformToUni(envelope -> sinkAdapter.emitOne(envelope, context));
    }

    /**
     * Executes a 1 -> N flow.
     */
    public static <E, I, O, R> R invokeOneToMany(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        return invokeOneToMany(event, context, sourceAdapter, invokeAdapter, sinkAdapter, DEFAULT_WAIT_TIMEOUT);
    }

    /**
     * Executes a 1 -> N flow with explicit timeout.
     */
    public static <E, I, O, R> R invokeOneToMany(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter,
            Duration timeout) {
        validateTimeout(timeout);
        return invokeOneToManyReactive(event, context, sourceAdapter, invokeAdapter, sinkAdapter)
            .await().atMost(timeout);
    }

    /**
     * Executes a non-blocking reactive 1 -> N flow and returns a {@link Uni} for the sink result.
     *
     * @param event inbound event payload
     * @param context function transport context
     * @param sourceAdapter source adapter
     * @param invokeAdapter invoke adapter
     * @param sinkAdapter sink adapter
     * @param <E> event type
     * @param <I> inbound payload type
     * @param <O> outbound payload type
     * @param <R> sink result type
     * @return reactive sink result
     */
    public static <E, I, O, R> Uni<R> invokeOneToManyReactive(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        validateParameters(event, context, sourceAdapter, invokeAdapter, sinkAdapter);

        Uni<TraceEnvelope<I>> inbound = requireSingleSource(event, context, sourceAdapter);

        Multi<TraceEnvelope<O>> outbound = inbound.onItem()
            .transformToMulti(envelope -> invokeAdapter.invokeOneToMany(envelope, context));

        return sinkAdapter.emitMany(outbound, context);
    }

    /**
     * Executes an N -> 1 flow.
     */
    public static <E, I, O, R> R invokeManyToOne(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        return invokeManyToOne(event, context, sourceAdapter, invokeAdapter, sinkAdapter, DEFAULT_WAIT_TIMEOUT);
    }

    /**
     * Executes an N -> 1 flow with explicit timeout.
     */
    public static <E, I, O, R> R invokeManyToOne(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter,
            Duration timeout) {
        validateTimeout(timeout);
        return invokeManyToOneReactive(event, context, sourceAdapter, invokeAdapter, sinkAdapter)
            .await().atMost(timeout);
    }

    /**
     * Executes a non-blocking reactive N -> 1 flow and returns a {@link Uni} for the sink result.
     *
     * @param event inbound event payload
     * @param context function transport context
     * @param sourceAdapter source adapter
     * @param invokeAdapter invoke adapter
     * @param sinkAdapter sink adapter
     * @param <E> event type
     * @param <I> inbound payload type
     * @param <O> outbound payload type
     * @param <R> sink result type
     * @return reactive sink result
     */
    public static <E, I, O, R> Uni<R> invokeManyToOneReactive(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        validateParameters(event, context, sourceAdapter, invokeAdapter, sinkAdapter);

        Multi<TraceEnvelope<I>> inbound = sourceAdapter.adapt(event, context);
        Uni<TraceEnvelope<O>> outbound = invokeAdapter.invokeManyToOne(inbound, context);

        return outbound.onItem()
            .transformToUni(envelope -> sinkAdapter.emitOne(envelope, context));
    }

    /**
     * Executes an N -> M flow.
     */
    public static <E, I, O, R> R invokeManyToMany(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        return invokeManyToMany(event, context, sourceAdapter, invokeAdapter, sinkAdapter, DEFAULT_WAIT_TIMEOUT);
    }

    /**
     * Executes an N -> M flow with explicit timeout.
     */
    public static <E, I, O, R> R invokeManyToMany(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter,
            Duration timeout) {
        validateTimeout(timeout);
        return invokeManyToManyReactive(event, context, sourceAdapter, invokeAdapter, sinkAdapter)
            .await().atMost(timeout);
    }

    /**
     * Executes a non-blocking reactive N -> M flow and returns a {@link Uni} for the sink result.
     *
     * @param event inbound event payload
     * @param context function transport context
     * @param sourceAdapter source adapter
     * @param invokeAdapter invoke adapter
     * @param sinkAdapter sink adapter
     * @param <E> event type
     * @param <I> inbound payload type
     * @param <O> outbound payload type
     * @param <R> sink result type
     * @return reactive sink result
     */
    public static <E, I, O, R> Uni<R> invokeManyToManyReactive(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        validateParameters(event, context, sourceAdapter, invokeAdapter, sinkAdapter);

        Multi<TraceEnvelope<I>> inbound = sourceAdapter.adapt(event, context);
        Multi<TraceEnvelope<O>> outbound = invokeAdapter.invokeManyToMany(inbound, context);
        return sinkAdapter.emitMany(outbound, context);
    }

    private static <T> T requireExactlyOne(List<T> items, String stage) {
        if (items.isEmpty()) {
            throw new IllegalStateException(
                "Function transport expected exactly one " + stage + " item but received 0.");
        }
        if (items.size() > 1) {
            throw new IllegalStateException(
                "Function transport expected exactly one " + stage + " item but received " + items.size() + ".");
        }
        return items.get(0);
    }

    private static <E, I> Uni<TraceEnvelope<I>> requireSingleSource(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter) {
        return sourceAdapter.adapt(event, context)
            .select().first(2)
            .collect().asList()
            .onItem().transform(items -> requireExactlyOne(items, "source"));
    }

    private static <E, I, O, R> void validateParameters(
            E event,
            FunctionTransportContext context,
            FunctionSourceAdapter<E, I> sourceAdapter,
            FunctionInvokeAdapter<I, O> invokeAdapter,
            FunctionSinkAdapter<O, R> sinkAdapter) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(sourceAdapter, "sourceAdapter must not be null");
        Objects.requireNonNull(invokeAdapter, "invokeAdapter must not be null");
        Objects.requireNonNull(sinkAdapter, "sinkAdapter must not be null");
    }

    private static void validateTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
    }
}
