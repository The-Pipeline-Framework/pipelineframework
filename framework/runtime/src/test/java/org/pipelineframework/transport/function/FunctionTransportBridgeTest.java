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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class FunctionTransportBridgeTest {
    private FunctionTransportContext context;

    @BeforeEach
    void setUp() {
        context = FunctionTransportContext.of("req-base", "search-handler", "invoke-step");
    }

    @Test
    void executesOneToManyFlow() {
        List<Integer> payloads = FunctionTransportBridge.invokeOneToMany(
            "hello",
            context,
            createUnarySourceAdapter("search.raw-document", "v1"),
            createOneToManyInvokeAdapter(),
            listSinkAdapter());
        assertEquals(List.of(5, 6), payloads);
    }

    @Test
    void executesManyToOneFlow() {
        Integer result = FunctionTransportBridge.invokeManyToOne(
            "ignored-event",
            context,
            createStreamingSourceAdapter(2, 3),
            createManyToOneInvokeAdapter(),
            unarySinkAdapter());
        assertEquals(5, result);
    }

    @Test
    void executesManyToManyFlow() {
        List<Integer> result = FunctionTransportBridge.invokeManyToMany(
            "ignored-event",
            context,
            createStreamingSourceAdapter(1, 2),
            createManyToManyInvokeAdapter(),
            listSinkAdapter());
        assertEquals(List.of(10, 20), result);
    }

    @Test
    void rejectsOneToManyWhenSourceProducesMultipleItems() {
        FunctionSourceAdapter<String, String> source = (event, ctx) -> Multi.createFrom().items(
            TraceEnvelope.root("trace-1", "item-1", "search.raw-document", "v1", "idem-1", event),
            TraceEnvelope.root("trace-1", "item-2", "search.raw-document", "v1", "idem-2", event));

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> FunctionTransportBridge.invokeOneToMany(
                "hello", context, source, createOneToManyInvokeAdapter(), listSinkAdapter()));
        assertEquals("Function transport expected exactly one source item but received 2.", ex.getMessage());
    }

    @Test
    void rejectsEmptySourceForOneToMany() {
        FunctionSourceAdapter<String, String> source = (event, ctx) -> Multi.createFrom().empty();

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> FunctionTransportBridge.invokeOneToMany(
                "hello", context, source, createOneToManyInvokeAdapter(), listSinkAdapter()));
        assertEquals("Function transport expected exactly one source item but received 0.", ex.getMessage());
    }

    @Test
    void handlesEmptySourceForManyToOne() {
        FunctionSourceAdapter<String, Integer> source = (event, ctx) -> Multi.createFrom().empty();
        Integer result = FunctionTransportBridge.invokeManyToOne(
            "event", context, source, createManyToOneInvokeAdapter(), unarySinkAdapter());
        assertEquals(0, result);
    }

    @Test
    void handlesEmptySourceForManyToMany() {
        FunctionSourceAdapter<String, Integer> source = (event, ctx) -> Multi.createFrom().empty();
        List<Integer> result = FunctionTransportBridge.invokeManyToMany(
            "event", context, source, createManyToManyInvokeAdapter(), listSinkAdapter());
        assertEquals(List.of(), result);
    }

    @Test
    void appliesDropOverflowPolicyForStreamingSinkInManyToManyFlow() {
        CollectListFunctionSinkAdapter<Integer> sink = new CollectListFunctionSinkAdapter<>(
            new BatchingPolicy(2, 1024, Duration.ofMillis(50), 1, BatchOverflowPolicy.DROP));

        List<Integer> result = FunctionTransportBridge.invokeManyToMany(
            "event",
            context,
            createStreamingSourceAdapter(1, 2, 3),
            createManyToManyInvokeAdapter(),
            sink);

        assertEquals(List.of(10, 20), result);
    }

    @Test
    void appliesFailOverflowPolicyForStreamingSinkInManyToManyFlow() {
        CollectListFunctionSinkAdapter<Integer> sink = new CollectListFunctionSinkAdapter<>(
            new BatchingPolicy(2, 1024, Duration.ofMillis(50), 1, BatchOverflowPolicy.FAIL));

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> FunctionTransportBridge.invokeManyToMany(
                "event",
                context,
                createStreamingSourceAdapter(1, 2, 3),
                createManyToManyInvokeAdapter(),
                sink));

        assertEquals("Function sink overflow: received at least 3 items with maxItems=2 and overflowPolicy=FAIL", ex.getMessage());
    }

    @Test
    void propagatesInvokeExceptionsAcrossShapes() {
        RuntimeException boom = new RuntimeException("invoke boom");

        FunctionSourceAdapter<String, String> unarySource = createUnarySourceAdapter("search.raw", "v1");
        FunctionSourceAdapter<String, Integer> streamingSource = createStreamingSourceAdapter(1);
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

        RuntimeException oneToMany = assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeOneToMany("event", context, unarySource, oneToManyInvoke, listSinkAdapter()));
        RuntimeException manyToOne = assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeManyToOne("event", context, streamingSource, manyToOneInvoke, unarySinkAdapter()));
        RuntimeException manyToMany = assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeManyToMany("event", context, streamingSource, manyToManyInvoke, listSinkAdapter()));

        assertSame(boom, oneToMany);
        assertSame(boom, manyToOne);
        assertSame(boom, manyToMany);
    }

    @Test
    void propagatesSinkExceptionsAcrossShapes() {
        RuntimeException sinkBoom = new RuntimeException("sink boom");

        FunctionSourceAdapter<String, String> unarySource = createUnarySourceAdapter("search.raw", "v1");
        FunctionSourceAdapter<String, Integer> streamingSource = createStreamingSourceAdapter(1);
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

        RuntimeException oneToMany = assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeOneToMany("event", context, unarySource, oneToManyInvoke, failingListSink));
        RuntimeException manyToOne = assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeManyToOne("event", context, streamingSource, manyToOneInvoke, failingUnarySink));
        RuntimeException manyToMany = assertThrows(RuntimeException.class,
            () -> FunctionTransportBridge.invokeManyToMany("event", context, streamingSource, manyToManyInvoke, failingListSink));

        assertSameOrCause(sinkBoom, oneToMany);
        assertSameOrCause(sinkBoom, manyToOne);
        assertSameOrCause(sinkBoom, manyToMany);
    }

    @Test
    void surfacesNullPayloadDereferenceInOneToMany() {
        FunctionTransportContext localContext = FunctionTransportContext.of("req-19a", "search-handler", "invoke-step");
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

        assertThrows(NullPointerException.class,
            () -> FunctionTransportBridge.invokeOneToMany(
                "event", localContext, nullPayloadSource, invokeUsesPayload, listSinkAdapter()));
    }

    @Test
    void allowsNullPayloadFlowInManyToOne() {
        FunctionTransportContext localContext = FunctionTransportContext.of("req-19b", "search-handler", "invoke-step");
        FunctionInvokeAdapter<Integer, Integer> manyToOneNullPayloadInvoke = new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeManyToOne(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return Uni.createFrom().item(TraceEnvelope.root("trace-1", "item-2", "search.out", "v1", "idem-2", null));
            }
        };

        Integer result = FunctionTransportBridge.invokeManyToOne(
            "event", localContext, createStreamingSourceAdapter(1), manyToOneNullPayloadInvoke, unarySinkAdapter());
        assertNull(result);
    }

    @Test
    void rejectsNonPositiveTimeoutForOneToOne() {
        IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
            () -> FunctionTransportBridge.invokeOneToOne(
                "event",
                context,
                createUnarySourceAdapter("search.raw-document", "v1"),
                new LocalUnaryFunctionInvokeAdapter<>(
                    payload -> Uni.createFrom().item(payload.length()),
                    "search.out",
                    "v1"),
                unarySinkAdapter(),
                Duration.ZERO));
        assertEquals("timeout must be > 0", zero.getMessage());

        IllegalArgumentException negative = assertThrows(IllegalArgumentException.class,
            () -> FunctionTransportBridge.invokeOneToOne(
                "event",
                context,
                createUnarySourceAdapter("search.raw-document", "v1"),
                new LocalUnaryFunctionInvokeAdapter<>(
                    payload -> Uni.createFrom().item(payload.length()),
                    "search.out",
                    "v1"),
                unarySinkAdapter(),
                Duration.ofMillis(-1)));
        assertEquals("timeout must be > 0", negative.getMessage());
    }

    private FunctionSourceAdapter<String, String> createUnarySourceAdapter(String model, String version) {
        return (event, ctx) -> Multi.createFrom().item(
            TraceEnvelope.root("trace-1", "item-1", model, version, "idem-1", event));
    }

    private FunctionSourceAdapter<String, Integer> createStreamingSourceAdapter(Integer... payloads) {
        return (event, ctx) -> Multi.createFrom().items(payloads)
            .onItem().transform(payload ->
                TraceEnvelope.root("trace-1", "item-" + payload, "search.token", "v1", "idem-" + payload, payload));
    }

    private FunctionInvokeAdapter<String, Integer> createOneToManyInvokeAdapter() {
        return new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeOneToMany(
                    TraceEnvelope<String> input,
                    FunctionTransportContext invokeContext) {
                return Multi.createFrom().items(
                    input.next("item-2", "search.token", "v1", "idem-2", input.payload().length()),
                    input.next("item-3", "search.token", "v1", "idem-3", input.payload().length() + 1));
            }
        };
    }

    private FunctionInvokeAdapter<Integer, Integer> createManyToOneInvokeAdapter() {
        return new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeManyToOne(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext invokeContext) {
                return input.onItem().transform(TraceEnvelope::payload).collect().asList()
                    .onItem().transform(values -> {
                        int sum = values.stream().mapToInt(Integer::intValue).sum();
                        return TraceEnvelope.root("trace-1", "item-sum", "search.sum", "v1", "idem-sum", sum);
                    });
            }
        };
    }

    private FunctionInvokeAdapter<Integer, Integer> createManyToManyInvokeAdapter() {
        return new FunctionInvokeAdapter<>() {
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
    }

    private FunctionSinkAdapter<Integer, List<Integer>> listSinkAdapter() {
        return (items, sinkContext) -> items.onItem().transform(TraceEnvelope::payload).collect().asList();
    }

    private FunctionSinkAdapter<Integer, Integer> unarySinkAdapter() {
        return new DefaultUnaryFunctionSinkAdapter<>();
    }

    private void assertSameOrCause(RuntimeException expected, RuntimeException actual) {
        Throwable cursor = actual;
        while (cursor != null) {
            if (cursor == expected) {
                return;
            }
            cursor = cursor.getCause();
        }
        fail("Expected throwable instance was not found in cause chain. expected="
            + expected + ", actual=" + actual);
    }
}
