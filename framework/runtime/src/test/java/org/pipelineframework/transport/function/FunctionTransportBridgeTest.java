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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FunctionTransportBridgeTest {

    @Test
    void executesOneToManyFlow() {
        FunctionTransportContext context = FunctionTransportContext.of("req-10", "search-handler", "invoke-step");
        FunctionSourceAdapter<String, String> source = (event, ctx) ->
            Multi.createFrom().item(TraceEnvelope.root("trace-1", "item-1", "search.raw-document", "v1", "idem-1", event));
        FunctionInvokeAdapter<String, Integer> invoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeOneToMany(
                    TraceEnvelope<String> input,
                    FunctionTransportContext invokeContext) {
                return Multi.createFrom().items(
                    input.next("item-2", "search.token", "v1", "idem-1", input.payload().length()),
                    input.next("item-3", "search.token", "v1", "idem-1", input.payload().length() + 1));
            }
        };
        FunctionSinkAdapter<Integer, List<Integer>> sink = (items, sinkContext) ->
            items.onItem().transform(TraceEnvelope::payload).collect().asList();

        List<Integer> payloads = FunctionTransportBridge.invokeOneToMany("hello", context, source, invoke, sink);
        assertEquals(List.of(5, 6), payloads);
    }

    @Test
    void executesManyToOneFlow() {
        FunctionTransportContext context = FunctionTransportContext.of("req-11", "search-handler", "invoke-step");
        FunctionSourceAdapter<String, Integer> source = (event, ctx) -> Multi.createFrom().items(
            TraceEnvelope.root("trace-1", "item-1", "search.token", "v1", "idem-1", 2),
            TraceEnvelope.root("trace-1", "item-2", "search.token", "v1", "idem-2", 3));
        FunctionInvokeAdapter<Integer, Integer> invoke = new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeManyToOne(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return input.onItem().transform(TraceEnvelope::payload).collect().asList()
                    .onItem().transform(values -> {
                        int sum = values.stream().mapToInt(Integer::intValue).sum();
                        return TraceEnvelope.root("trace-1", "item-3", "search.sum", "v1", "idem-3", sum);
                    });
            }
        };
        FunctionSinkAdapter<Integer, Integer> sink = new DefaultUnaryFunctionSinkAdapter<>();

        Integer result = FunctionTransportBridge.invokeManyToOne("ignored-event", context, source, invoke, sink);
        assertEquals(5, result);
    }

    @Test
    void executesManyToManyFlow() {
        FunctionTransportContext context = FunctionTransportContext.of("req-12", "search-handler", "invoke-step");
        FunctionSourceAdapter<String, Integer> source = (event, ctx) -> Multi.createFrom().items(
            TraceEnvelope.root("trace-1", "item-1", "search.token", "v1", "idem-1", 1),
            TraceEnvelope.root("trace-1", "item-2", "search.token", "v1", "idem-2", 2));
        FunctionInvokeAdapter<Integer, Integer> invoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeManyToMany(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return input.onItem().transform(envelope -> envelope.next(
                    envelope.itemId() + "-next",
                    "search.token.out",
                    "v1",
                    envelope.idempotencyKey(),
                    envelope.payload() * 10));
            }
        };
        FunctionSinkAdapter<Integer, List<Integer>> sink = (items, sinkContext) ->
            items.onItem().transform(TraceEnvelope::payload).collect().asList();

        List<Integer> result = FunctionTransportBridge.invokeManyToMany("ignored-event", context, source, invoke, sink);
        assertEquals(List.of(10, 20), result);
    }

    @Test
    void rejectsOneToManyWhenSourceProducesMultipleItems() {
        FunctionTransportContext context = FunctionTransportContext.of("req-13", "search-handler", "invoke-step");
        FunctionSourceAdapter<String, String> source = (event, ctx) -> Multi.createFrom().items(
            TraceEnvelope.root("trace-1", "item-1", "search.raw-document", "v1", "idem-1", event),
            TraceEnvelope.root("trace-1", "item-2", "search.raw-document", "v1", "idem-2", event));
        FunctionInvokeAdapter<String, Integer> invoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeOneToMany(
                    TraceEnvelope<String> input,
                    FunctionTransportContext invokeContext) {
                return Multi.createFrom().item(input.next("item-3", "search.token", "v1", "idem-3", 1));
            }
        };
        FunctionSinkAdapter<Integer, List<Integer>> sink = (items, sinkContext) ->
            items.onItem().transform(TraceEnvelope::payload).collect().asList();

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> FunctionTransportBridge.invokeOneToMany("hello", context, source, invoke, sink));
        assertEquals("Function transport expected exactly one source item but received 2.", ex.getMessage());
    }

    @Test
    void rejectsEmptySourceForOneToMany() {
        FunctionTransportContext context = FunctionTransportContext.of("req-14", "search-handler", "invoke-step");
        FunctionSourceAdapter<String, String> source = (event, ctx) -> Multi.createFrom().empty();
        FunctionInvokeAdapter<String, Integer> invoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeOneToMany(
                    TraceEnvelope<String> input,
                    FunctionTransportContext invokeContext) {
                return Multi.createFrom().empty();
            }
        };
        FunctionSinkAdapter<Integer, List<Integer>> sink = (items, sinkContext) ->
            items.onItem().transform(TraceEnvelope::payload).collect().asList();

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> FunctionTransportBridge.invokeOneToMany("hello", context, source, invoke, sink));
        assertEquals("Function transport expected exactly one source item but received 0.", ex.getMessage());
    }

    @Test
    void handlesEmptySourceForManyToOne() {
        FunctionTransportContext context = FunctionTransportContext.of("req-15", "search-handler", "invoke-step");
        FunctionSourceAdapter<String, Integer> source = (event, ctx) -> Multi.createFrom().empty();
        FunctionInvokeAdapter<Integer, Integer> invoke = new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeManyToOne(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return input.collect().asList().onItem().transform(values ->
                    TraceEnvelope.root("trace-1", "item-empty", "search.sum", "v1", "idem-empty", values.size()));
            }
        };
        FunctionSinkAdapter<Integer, Integer> sink = new DefaultUnaryFunctionSinkAdapter<>();

        Integer result = FunctionTransportBridge.invokeManyToOne("event", context, source, invoke, sink);
        assertEquals(0, result);
    }

    @Test
    void handlesEmptySourceForManyToMany() {
        FunctionTransportContext context = FunctionTransportContext.of("req-16", "search-handler", "invoke-step");
        FunctionSourceAdapter<String, Integer> source = (event, ctx) -> Multi.createFrom().empty();
        FunctionInvokeAdapter<Integer, Integer> invoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeManyToMany(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return Multi.createFrom().empty();
            }
        };
        FunctionSinkAdapter<Integer, List<Integer>> sink = (items, sinkContext) ->
            items.onItem().transform(TraceEnvelope::payload).collect().asList();

        List<Integer> result = FunctionTransportBridge.invokeManyToMany("event", context, source, invoke, sink);
        assertEquals(List.of(), result);
    }

    @Test
    void propagatesInvokeExceptionsAcrossShapes() {
        FunctionTransportContext context = FunctionTransportContext.of("req-17", "search-handler", "invoke-step");
        RuntimeException boom = new RuntimeException("invoke boom");

        FunctionSourceAdapter<String, String> unarySource = (event, ctx) ->
            Multi.createFrom().item(TraceEnvelope.root("trace-1", "item-1", "search.raw", "v1", "idem-1", event));
        FunctionSourceAdapter<String, Integer> streamingSource = (event, ctx) -> Multi.createFrom().items(
            TraceEnvelope.root("trace-1", "item-1", "search.raw", "v1", "idem-1", 1));

        FunctionInvokeAdapter<String, Integer> oneToManyInvoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeOneToMany(
                    TraceEnvelope<String> input,
                    FunctionTransportContext invokeContext) {
                throw boom;
            }
        };
        FunctionInvokeAdapter<Integer, Integer> manyToOneInvoke = new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeManyToOne(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                throw boom;
            }
        };
        FunctionInvokeAdapter<Integer, Integer> manyToManyInvoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeManyToMany(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                throw boom;
            }
        };

        FunctionSinkAdapter<Integer, List<Integer>> listSink = (items, sinkContext) ->
            items.onItem().transform(TraceEnvelope::payload).collect().asList();
        FunctionSinkAdapter<Integer, Integer> unarySink = new DefaultUnaryFunctionSinkAdapter<>();

        assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeOneToMany("event", context, unarySource, oneToManyInvoke, listSink));
        assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeManyToOne("event", context, streamingSource, manyToOneInvoke, unarySink));
        assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeManyToMany("event", context, streamingSource, manyToManyInvoke, listSink));
    }

    @Test
    void propagatesSinkExceptionsAcrossShapes() {
        FunctionTransportContext context = FunctionTransportContext.of("req-18", "search-handler", "invoke-step");
        RuntimeException sinkBoom = new RuntimeException("sink boom");

        FunctionSourceAdapter<String, String> unarySource = (event, ctx) ->
            Multi.createFrom().item(TraceEnvelope.root("trace-1", "item-1", "search.raw", "v1", "idem-1", event));
        FunctionSourceAdapter<String, Integer> streamingSource = (event, ctx) -> Multi.createFrom().items(
            TraceEnvelope.root("trace-1", "item-1", "search.raw", "v1", "idem-1", 1));
        FunctionInvokeAdapter<String, Integer> oneToManyInvoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeOneToMany(
                    TraceEnvelope<String> input,
                    FunctionTransportContext invokeContext) {
                return Multi.createFrom().item(input.next("item-2", "search.out", "v1", "idem-2", 1));
            }
        };
        FunctionInvokeAdapter<Integer, Integer> manyToOneInvoke = new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeManyToOne(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return Uni.createFrom().item(TraceEnvelope.root("trace-1", "item-2", "search.out", "v1", "idem-2", 1));
            }
        };
        FunctionInvokeAdapter<Integer, Integer> manyToManyInvoke = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeManyToMany(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return Multi.createFrom().item(TraceEnvelope.root("trace-1", "item-2", "search.out", "v1", "idem-2", 1));
            }
        };
        FunctionSinkAdapter<Integer, List<Integer>> failingListSink = (items, sinkContext) ->
            Uni.createFrom().failure(sinkBoom);
        FunctionSinkAdapter<Integer, Integer> failingUnarySink = (items, sinkContext) ->
            Uni.createFrom().failure(sinkBoom);

        assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeOneToMany("event", context, unarySource, oneToManyInvoke, failingListSink));
        assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeManyToOne("event", context, streamingSource, manyToOneInvoke, failingUnarySink));
        assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeManyToMany("event", context, streamingSource, manyToManyInvoke, failingListSink));
    }

    @Test
    void surfacesNullPayloadHandling() {
        FunctionTransportContext context = FunctionTransportContext.of("req-19", "search-handler", "invoke-step");
        FunctionSourceAdapter<String, String> nullPayloadSource = (event, ctx) ->
            Multi.createFrom().item(TraceEnvelope.root("trace-1", "item-1", "search.raw", "v1", "idem-1", null));
        FunctionInvokeAdapter<String, Integer> invokeUsesPayload = new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeOneToMany(
                    TraceEnvelope<String> input,
                    FunctionTransportContext invokeContext) {
                return Multi.createFrom().item(input.next("item-2", "search.out", "v1", "idem-2", input.payload().length()));
            }
        };
        FunctionSinkAdapter<Integer, List<Integer>> listSink = (items, sinkContext) ->
            items.onItem().transform(TraceEnvelope::payload).collect().asList();

        assertThrows(NullPointerException.class,
            () -> FunctionTransportBridge.invokeOneToMany("event", context, nullPayloadSource, invokeUsesPayload, listSink));

        FunctionSinkAdapter<Integer, Integer> unarySink = new DefaultUnaryFunctionSinkAdapter<>();
        FunctionInvokeAdapter<Integer, Integer> manyToOneNullPayloadInvoke = new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeManyToOne(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return Uni.createFrom().item(TraceEnvelope.root("trace-1", "item-2", "search.out", "v1", "idem-2", null));
            }
        };
        FunctionSourceAdapter<String, Integer> source = (event, ctx) ->
            Multi.createFrom().item(TraceEnvelope.root("trace-1", "item-1", "search.raw", "v1", "idem-1", 1));

        Integer result = FunctionTransportBridge.invokeManyToOne("event", context, source, manyToOneNullPayloadInvoke, unarySink);
        assertNull(result);
    }
}
