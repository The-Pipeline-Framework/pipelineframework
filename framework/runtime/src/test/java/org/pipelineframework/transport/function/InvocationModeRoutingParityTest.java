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
import java.util.Map;
import java.util.function.Function;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InvocationModeRoutingParityTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    @Test
    void parityOneToOneAcrossLocalAndRemoteModes() {
        InvocationModeRoutingFunctionInvokeAdapter<Integer, Integer> routing = new InvocationModeRoutingFunctionInvokeAdapter<>(
            unaryDelegate(payload -> payload + 1),
            unaryDelegate(payload -> payload + 1));
        TraceEnvelope<Integer> input = TraceEnvelope.root("trace-1", "item-1", "search.in", "v1", "idem-1", 10);

        TraceEnvelope<Integer> local = routing.invokeOneToOne(input, localContext())
            .await().atMost(TIMEOUT);
        TraceEnvelope<Integer> remote = routing.invokeOneToOne(input, remoteContext())
            .await().atMost(TIMEOUT);

        assertEquals(local.payload(), remote.payload());
        assertEquals(local.payloadModel(), remote.payloadModel());
        assertEquals(local.payloadModelVersion(), remote.payloadModelVersion());
    }

    @Test
    void parityOneToManyAcrossLocalAndRemoteModes() {
        InvocationModeRoutingFunctionInvokeAdapter<Integer, Integer> routing = new InvocationModeRoutingFunctionInvokeAdapter<>(
            oneToManyDelegate(payload -> List.of(payload, payload + 1)),
            oneToManyDelegate(payload -> List.of(payload, payload + 1)));
        TraceEnvelope<Integer> input = TraceEnvelope.root("trace-2", "item-2", "search.in", "v1", "idem-2", 7);

        List<Integer> local = routing.invokeOneToMany(input, localContext())
            .onItem().transform(TraceEnvelope::payload)
            .collect().asList().await().atMost(TIMEOUT);
        List<Integer> remote = routing.invokeOneToMany(input, remoteContext())
            .onItem().transform(TraceEnvelope::payload)
            .collect().asList().await().atMost(TIMEOUT);

        assertEquals(local, remote);
    }

    @Test
    void parityManyToOneAcrossLocalAndRemoteModes() {
        InvocationModeRoutingFunctionInvokeAdapter<Integer, Integer> routing = new InvocationModeRoutingFunctionInvokeAdapter<>(
            manyToOneDelegate(items -> items.stream().mapToInt(Integer::intValue).sum()),
            manyToOneDelegate(items -> items.stream().mapToInt(Integer::intValue).sum()));
        Multi<TraceEnvelope<Integer>> input = Multi.createFrom().items(
            TraceEnvelope.root("trace-3", "item-3a", "search.in", "v1", "idem-3a", 2),
            TraceEnvelope.root("trace-3", "item-3b", "search.in", "v1", "idem-3b", 3));

        Integer local = routing.invokeManyToOne(input, localContext())
            .onItem().transform(TraceEnvelope::payload)
            .await().atMost(TIMEOUT);

        Multi<TraceEnvelope<Integer>> inputRemote = Multi.createFrom().items(
            TraceEnvelope.root("trace-3", "item-3a", "search.in", "v1", "idem-3a", 2),
            TraceEnvelope.root("trace-3", "item-3b", "search.in", "v1", "idem-3b", 3));
        Integer remote = routing.invokeManyToOne(inputRemote, remoteContext())
            .onItem().transform(TraceEnvelope::payload)
            .await().atMost(TIMEOUT);

        assertEquals(local, remote);
    }

    @Test
    void parityManyToManyAcrossLocalAndRemoteModes() {
        InvocationModeRoutingFunctionInvokeAdapter<Integer, Integer> routing = new InvocationModeRoutingFunctionInvokeAdapter<>(
            manyToManyDelegate(payload -> payload * 10),
            manyToManyDelegate(payload -> payload * 10));
        Multi<TraceEnvelope<Integer>> input = Multi.createFrom().items(
            TraceEnvelope.root("trace-4", "item-4a", "search.in", "v1", "idem-4a", 1),
            TraceEnvelope.root("trace-4", "item-4b", "search.in", "v1", "idem-4b", 2));

        List<Integer> local = routing.invokeManyToMany(input, localContext())
            .onItem().transform(TraceEnvelope::payload)
            .collect().asList().await().atMost(TIMEOUT);

        Multi<TraceEnvelope<Integer>> inputRemote = Multi.createFrom().items(
            TraceEnvelope.root("trace-4", "item-4a", "search.in", "v1", "idem-4a", 1),
            TraceEnvelope.root("trace-4", "item-4b", "search.in", "v1", "idem-4b", 2));
        List<Integer> remote = routing.invokeManyToMany(inputRemote, remoteContext())
            .onItem().transform(TraceEnvelope::payload)
            .collect().asList().await().atMost(TIMEOUT);

        assertEquals(local, remote);
    }

    @Test
    void parityFailureSemanticsAcrossModes() {
        RuntimeException boom = new RuntimeException("invoke-failure");
        InvocationModeRoutingFunctionInvokeAdapter<Integer, Integer> routing = new InvocationModeRoutingFunctionInvokeAdapter<>(
            failingUnaryDelegate(boom),
            failingUnaryDelegate(boom));
        TraceEnvelope<Integer> input = TraceEnvelope.root("trace-5", "item-5", "search.in", "v1", "idem-5", 1);

        RuntimeException local = assertThrows(RuntimeException.class,
            () -> routing.invokeOneToOne(input, localContext()).await().atMost(TIMEOUT));
        RuntimeException remote = assertThrows(RuntimeException.class,
            () -> routing.invokeOneToOne(input, remoteContext()).await().atMost(TIMEOUT));

        assertEquals("invoke-failure", local.getMessage());
        assertEquals("invoke-failure", remote.getMessage());
    }

    @Test
    void remoteModeWithoutConfiguredAdapterFailsExplicitly() {
        InvocationModeRoutingFunctionInvokeAdapter<Integer, Integer> routing = new InvocationModeRoutingFunctionInvokeAdapter<>(
            unaryDelegate(payload -> payload + 1),
            new UnsupportedRemoteFunctionInvokeAdapter<>());
        TraceEnvelope<Integer> input = TraceEnvelope.root("trace-6", "item-6", "search.in", "v1", "idem-6", 5);

        UnsupportedOperationException error = assertThrows(UnsupportedOperationException.class,
            () -> routing.invokeOneToOne(input, remoteContext()).await().atMost(TIMEOUT));
        assertTrue(error.getMessage().contains("Function invocation mode is REMOTE"));
        assertTrue(error.getMessage().contains("module=index-document-svc"));
    }

    private FunctionTransportContext localContext() {
        return FunctionTransportContext.of("req-local", "search-handler", "invoke-step");
    }

    private FunctionTransportContext remoteContext() {
        return FunctionTransportContext.of(
            "req-remote",
            "search-handler",
            "invoke-step",
            Map.of(
                FunctionTransportContext.ATTR_INVOCATION_MODE, "REMOTE",
                FunctionTransportContext.ATTR_TARGET_RUNTIME, "pipeline",
                FunctionTransportContext.ATTR_TARGET_MODULE, "index-document-svc",
                FunctionTransportContext.ATTR_TARGET_HANDLER, "ProcessIndexDocumentFunctionHandler"));
    }

    private FunctionInvokeAdapter<Integer, Integer> unaryDelegate(Function<Integer, Integer> mapper) {
        return new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeOneToOne(
                    TraceEnvelope<Integer> input,
                    FunctionTransportContext context) {
                return Uni.createFrom().item(input.next(
                    input.itemId() + "-n",
                    "search.out",
                    "v1",
                    input.idempotencyKey(),
                    mapper.apply(input.payload())));
            }
        };
    }

    private FunctionInvokeAdapter<Integer, Integer> failingUnaryDelegate(RuntimeException error) {
        return new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeOneToOne(
                    TraceEnvelope<Integer> input,
                    FunctionTransportContext context) {
                return Uni.createFrom().failure(error);
            }
        };
    }

    private FunctionInvokeAdapter<Integer, Integer> oneToManyDelegate(Function<Integer, List<Integer>> mapper) {
        return new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeOneToMany(
                    TraceEnvelope<Integer> input,
                    FunctionTransportContext context) {
                return Multi.createFrom().iterable(mapper.apply(input.payload()))
                    .onItem().transform(item -> input.next(
                        input.itemId() + "-n-" + item,
                        "search.out",
                        "v1",
                        input.idempotencyKey() + ":" + item,
                        item));
            }
        };
    }

    private FunctionInvokeAdapter<Integer, Integer> manyToOneDelegate(Function<List<Integer>, Integer> reducer) {
        return new FunctionInvokeAdapter<>() {
            @Override
            public Uni<TraceEnvelope<Integer>> invokeManyToOne(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext context) {
                return input.collect().asList().onItem().transform(items -> {
                    int reduced = reducer.apply(items.stream().map(TraceEnvelope::payload).toList());
                    TraceEnvelope<Integer> first = items.get(0);
                    return first.next(
                        first.itemId() + "-m1",
                        "search.out",
                        "v1",
                        first.idempotencyKey() + ":m1",
                        reduced);
                });
            }
        };
    }

    private FunctionInvokeAdapter<Integer, Integer> manyToManyDelegate(Function<Integer, Integer> mapper) {
        return new FunctionInvokeAdapter<>() {
            @Override
            public Multi<TraceEnvelope<Integer>> invokeManyToMany(
                    Multi<TraceEnvelope<Integer>> input,
                    FunctionTransportContext context) {
                return input.onItem().transform(item -> item.next(
                    item.itemId() + "-mm",
                    "search.out",
                    "v1",
                    item.idempotencyKey() + ":mm",
                    mapper.apply(item.payload())));
            }
        };
    }
}
