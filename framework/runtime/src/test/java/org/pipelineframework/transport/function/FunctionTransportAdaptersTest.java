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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FunctionTransportAdaptersTest {

    @Test
    void multiSourceAdapterReusesEnvelopeIdInIdempotencyKey() {
        MultiFunctionSourceAdapter<Integer> adapter = new MultiFunctionSourceAdapter<>("search.token", "v1");
        FunctionTransportContext context = new FunctionTransportContext(
            "req-101",
            "search-handler",
            "ingress",
            java.util.Map.of("custom", "abc"));

        List<TraceEnvelope<Integer>> envelopes = adapter.adapt(Multi.createFrom().items(1, 2, 3), context)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(3, envelopes.size());
        for (TraceEnvelope<Integer> envelope : envelopes) {
            assertEquals("req-101", envelope.traceId());
            assertNotNull(envelope.itemId());
            assertEquals("req-101:search.token:" + envelope.itemId(), envelope.idempotencyKey());
            assertEquals("search-handler", envelope.meta().get("functionName"));
            assertEquals("ingress", envelope.meta().get("stage"));
            assertEquals("req-101", envelope.meta().get("requestId"));
            assertEquals("abc", envelope.meta().get("custom"));
        }
    }

    @Test
    void localManyToManyAdapterGeneratesUniqueIdempotencyKeysPerOutput() {
        LocalManyToManyFunctionInvokeAdapter<Integer, Integer> adapter = new LocalManyToManyFunctionInvokeAdapter<>(
            payloads -> payloads.onItem().transform(x -> x * 10),
            "search.token.out",
            "v1");
        FunctionTransportContext context = FunctionTransportContext.of("req-202", "search-handler", "invoke-step");

        Multi<TraceEnvelope<Integer>> input = Multi.createFrom().items(
            TraceEnvelope.root("trace-a", "item-a", "search.token", "v1", "idem-a", 1),
            TraceEnvelope.root("trace-a", "item-b", "search.token", "v1", "idem-b", 2));

        List<TraceEnvelope<Integer>> outputs = adapter.invokeManyToMany(input, context)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(2, outputs.size());
        Set<String> keys = new HashSet<>();
        for (TraceEnvelope<Integer> output : outputs) {
            assertEquals("req-202", output.traceId());
            assertTrue(output.idempotencyKey().startsWith("req-202:search.token.out:"));
            keys.add(output.idempotencyKey());
        }
        assertEquals(2, keys.size());
    }

    @Test
    void localUnaryAdapterRejectsNullInputPayloadBeforeDelegateCall() {
        LocalUnaryFunctionInvokeAdapter<String, Integer> adapter = new LocalUnaryFunctionInvokeAdapter<>(
            payload -> Uni.createFrom().item(payload.length()),
            "search.out",
            "v1");
        TraceEnvelope<String> input = TraceEnvelope.root("trace-1", "item-1", "search.in", "v1", "idem-1", null);
        FunctionTransportContext context = FunctionTransportContext.of("req-303", "search-handler", "invoke-step");

        NullPointerException ex = assertThrows(
            NullPointerException.class,
            () -> adapter.invokeOneToOne(input, context).await().atMost(Duration.ofSeconds(2)));
        assertEquals("LocalUnaryFunctionInvokeAdapter input payload must not be null", ex.getMessage());
    }

    @Test
    void collectListSinkFailsWhenItemsStreamIsNull() {
        CollectListFunctionSinkAdapter<Integer> sink = new CollectListFunctionSinkAdapter<>();
        FunctionTransportContext context = FunctionTransportContext.of("req-404", "search-handler", "egress");

        NullPointerException ex = assertThrows(
            NullPointerException.class,
            () -> sink.emitMany(null, context).await().atMost(Duration.ofSeconds(2)));
        assertEquals("items must not be null", ex.getMessage());
    }

    @Test
    void localOneToManyAdapterWrapsDelegateOutputs() {
        LocalOneToManyFunctionInvokeAdapter<Integer, Integer> adapter = new LocalOneToManyFunctionInvokeAdapter<>(
            payload -> Multi.createFrom().items(payload, payload + 1),
            "search.token.out",
            "v1");
        TraceEnvelope<Integer> input = TraceEnvelope.root("trace-om", "item-om", "search.token", "v1", "idem-om", 7);
        FunctionTransportContext context = FunctionTransportContext.of("req-505", "search-handler", "invoke-step");

        List<TraceEnvelope<Integer>> outputs = adapter.invokeOneToMany(input, context)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(2, outputs.size());
        assertEquals(7, outputs.get(0).payload());
        assertEquals(8, outputs.get(1).payload());
        assertEquals("trace-om", outputs.get(0).traceId());
        assertEquals("trace-om", outputs.get(1).traceId());
    }

    @Test
    void localManyToOneAdapterCollapsesInputStreamToSingleEnvelope() {
        LocalManyToOneFunctionInvokeAdapter<Integer, Integer> adapter = new LocalManyToOneFunctionInvokeAdapter<>(
            payloads -> payloads.collect().asList().onItem().transform(list -> list.stream().mapToInt(Integer::intValue).sum()),
            "search.token.sum",
            "v1");
        FunctionTransportContext context = FunctionTransportContext.of("req-606", "search-handler", "invoke-step");
        Multi<TraceEnvelope<Integer>> input = Multi.createFrom().items(
            TraceEnvelope.root("trace-m1", "item-m1", "search.token", "v1", "idem-m1", 2),
            TraceEnvelope.root("trace-m1", "item-m2", "search.token", "v1", "idem-m2", 3));

        TraceEnvelope<Integer> output = adapter.invokeManyToOne(input, context)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(5, output.payload());
        assertEquals("trace-m1", output.traceId());
        assertEquals("search.token.sum", output.payloadModel());
        assertNotNull(output.previousItemRef());
    }

    @Test
    void localManyToOneAdapterFailsOnOverflowWhenPolicyIsFail() {
        BatchingPolicy policy = new BatchingPolicy(2, 1024, Duration.ofMillis(50), 1, BatchOverflowPolicy.FAIL);
        LocalManyToOneFunctionInvokeAdapter<Integer, Integer> adapter = new LocalManyToOneFunctionInvokeAdapter<>(
            payloads -> payloads.collect().asList().onItem().transform(list -> list.stream().mapToInt(Integer::intValue).sum()),
            "search.token.sum",
            "v1",
            policy);
        FunctionTransportContext context = FunctionTransportContext.of("req-607", "search-handler", "invoke-step");
        Multi<TraceEnvelope<Integer>> input = Multi.createFrom().items(
            TraceEnvelope.root("trace-m2", "item-1", "search.token", "v1", "idem-1", 1),
            TraceEnvelope.root("trace-m2", "item-2", "search.token", "v1", "idem-2", 2),
            TraceEnvelope.root("trace-m2", "item-3", "search.token", "v1", "idem-3", 3));

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> adapter.invokeManyToOne(input, context).await().atMost(Duration.ofSeconds(2)));
        assertEquals("Function invoke overflow: received 3 items with maxItems=2 and overflowPolicy=FAIL", ex.getMessage());
    }

    @Test
    void localManyToOneAdapterDropsOverflowWhenPolicyIsDrop() {
        BatchingPolicy policy = new BatchingPolicy(2, 1024, Duration.ofMillis(50), 1, BatchOverflowPolicy.DROP);
        LocalManyToOneFunctionInvokeAdapter<Integer, Integer> adapter = new LocalManyToOneFunctionInvokeAdapter<>(
            payloads -> payloads.collect().asList().onItem().transform(list -> list.stream().mapToInt(Integer::intValue).sum()),
            "search.token.sum",
            "v1",
            policy);
        FunctionTransportContext context = FunctionTransportContext.of("req-608", "search-handler", "invoke-step");
        Multi<TraceEnvelope<Integer>> input = Multi.createFrom().items(
            TraceEnvelope.root("trace-m3", "item-1", "search.token", "v1", "idem-1", 1),
            TraceEnvelope.root("trace-m3", "item-2", "search.token", "v1", "idem-2", 2),
            TraceEnvelope.root("trace-m3", "item-3", "search.token", "v1", "idem-3", 3));

        TraceEnvelope<Integer> output = adapter.invokeManyToOne(input, context)
            .await().atMost(Duration.ofSeconds(2));
        assertEquals(3, output.payload());
    }

    @Test
    void defaultFunctionInvokeAdapterMethodsFailByContract() {
        FunctionInvokeAdapter<String, Integer> adapter = new FunctionInvokeAdapter<>() {
        };
        FunctionTransportContext context = FunctionTransportContext.of("req-707", "search-handler", "invoke-step");
        TraceEnvelope<String> unaryInput = TraceEnvelope.root("trace-fi", "item-fi", "search.token", "v1", "idem-fi", "x");
        Multi<TraceEnvelope<String>> streamInput = Multi.createFrom().item(unaryInput);

        UnsupportedOperationException oneToOne = assertThrows(
            UnsupportedOperationException.class,
            () -> adapter.invokeOneToOne(unaryInput, context).await().atMost(Duration.ofSeconds(2)));
        UnsupportedOperationException oneToMany = assertThrows(
            UnsupportedOperationException.class,
            () -> adapter.invokeOneToMany(unaryInput, context).collect().asList().await().atMost(Duration.ofSeconds(2)));
        UnsupportedOperationException manyToOne = assertThrows(
            UnsupportedOperationException.class,
            () -> adapter.invokeManyToOne(streamInput, context).await().atMost(Duration.ofSeconds(2)));
        UnsupportedOperationException manyToMany = assertThrows(
            UnsupportedOperationException.class,
            () -> adapter.invokeManyToMany(streamInput, context).collect().asList().await().atMost(Duration.ofSeconds(2)));

        assertEquals("1->1 invocation is not implemented", oneToOne.getMessage());
        assertEquals("1->N invocation is not implemented", oneToMany.getMessage());
        assertEquals("N->1 invocation is not implemented", manyToOne.getMessage());
        assertEquals("N->M invocation is not implemented", manyToMany.getMessage());
    }

    @Test
    void batchingDefaultsReferenceBufferOverflowPolicy() {
        BatchingPolicy policy = BatchingPolicy.defaultPolicy();
        BatchOverflowPolicy[] values = BatchOverflowPolicy.values();

        assertEquals(BatchOverflowPolicy.BUFFER, policy.overflowPolicy());
        assertTrue(List.of(values).contains(BatchOverflowPolicy.BUFFER));
        assertTrue(List.of(values).contains(BatchOverflowPolicy.DROP));
        assertTrue(List.of(values).contains(BatchOverflowPolicy.FAIL));
    }

    @Test
    void multiSourceAdapterFailsOnOverflowWhenPolicyIsFail() {
        BatchingPolicy policy = new BatchingPolicy(2, 1024, Duration.ofMillis(50), 1, BatchOverflowPolicy.FAIL);
        MultiFunctionSourceAdapter<Integer> adapter = new MultiFunctionSourceAdapter<>("search.token", "v1", policy);
        FunctionTransportContext context = FunctionTransportContext.of("req-809", "search-handler", "ingress");

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> adapter.adapt(Multi.createFrom().items(1, 2, 3), context)
                .collect().asList().await().atMost(Duration.ofSeconds(2)));
        assertEquals("Function source overflow: received 3 items with maxItems=2 and overflowPolicy=FAIL", ex.getMessage());
    }

    @Test
    void multiSourceAdapterDropsOverflowWhenPolicyIsDrop() {
        BatchingPolicy policy = new BatchingPolicy(2, 1024, Duration.ofMillis(50), 1, BatchOverflowPolicy.DROP);
        MultiFunctionSourceAdapter<Integer> adapter = new MultiFunctionSourceAdapter<>("search.token", "v1", policy);
        FunctionTransportContext context = FunctionTransportContext.of("req-810", "search-handler", "ingress");

        List<TraceEnvelope<Integer>> envelopes = adapter.adapt(Multi.createFrom().items(1, 2, 3), context)
            .collect().asList().await().atMost(Duration.ofSeconds(2));
        assertEquals(2, envelopes.size());
        assertEquals(1, envelopes.get(0).payload());
        assertEquals(2, envelopes.get(1).payload());
    }

    @Test
    void multiSourceAdapterUsesExplicitIdempotencyKeyWhenConfigured() {
        MultiFunctionSourceAdapter<Integer> adapter = new MultiFunctionSourceAdapter<>("search.token", "v1");
        FunctionTransportContext context = new FunctionTransportContext(
            "req-explicit",
            "search-handler",
            "ingress",
            java.util.Map.of(
                FunctionTransportContext.ATTR_IDEMPOTENCY_POLICY, "EXPLICIT",
                FunctionTransportContext.ATTR_IDEMPOTENCY_KEY, "business-123"));

        List<TraceEnvelope<Integer>> envelopes = adapter.adapt(Multi.createFrom().items(10, 11), context)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals("business-123:0", envelopes.get(0).idempotencyKey());
        assertEquals("business-123:1", envelopes.get(1).idempotencyKey());
    }

    @Test
    void unarySourceAdapterUsesExplicitIdempotencyKeyWhenConfigured() {
        DefaultUnaryFunctionSourceAdapter<String> adapter = new DefaultUnaryFunctionSourceAdapter<>("search.raw", "v1");
        FunctionTransportContext context = new FunctionTransportContext(
            "req-explicit-unary",
            "search-handler",
            "ingress",
            java.util.Map.of(
                FunctionTransportContext.ATTR_IDEMPOTENCY_POLICY, "EXPLICIT",
                FunctionTransportContext.ATTR_IDEMPOTENCY_KEY, "csv-id-77"));

        List<TraceEnvelope<String>> envelopes = adapter.adapt("payload", context)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(1, envelopes.size());
        assertEquals("csv-id-77", envelopes.get(0).idempotencyKey());
    }

    @Test
    void localManyAdaptersUseExplicitIdempotencyPolicyWhenConfigured() {
        FunctionTransportContext context = new FunctionTransportContext(
            "req-explicit-local",
            "search-handler",
            "invoke-step",
            java.util.Map.of(
                FunctionTransportContext.ATTR_IDEMPOTENCY_POLICY, "EXPLICIT",
                FunctionTransportContext.ATTR_IDEMPOTENCY_KEY, "entity-900"));

        LocalManyToOneFunctionInvokeAdapter<Integer, Integer> manyToOne = new LocalManyToOneFunctionInvokeAdapter<>(
            payloads -> payloads.collect().asList().onItem().transform(list -> list.stream().mapToInt(Integer::intValue).sum()),
            "search.token.sum",
            "v1");
        TraceEnvelope<Integer> one = TraceEnvelope.root("trace-explicit", "item-1", "search.token", "v1", "idem-1", 1);
        TraceEnvelope<Integer> two = TraceEnvelope.root("trace-explicit", "item-2", "search.token", "v1", "idem-2", 2);
        TraceEnvelope<Integer> collapsed = manyToOne.invokeManyToOne(Multi.createFrom().items(one, two), context)
            .await().atMost(Duration.ofSeconds(2));
        assertEquals("entity-900", collapsed.idempotencyKey());

        LocalManyToManyFunctionInvokeAdapter<Integer, Integer> manyToMany = new LocalManyToManyFunctionInvokeAdapter<>(
            payloads -> payloads.onItem().transform(v -> v * 2),
            "search.token.out",
            "v1");
        List<TraceEnvelope<Integer>> expanded = manyToMany.invokeManyToMany(Multi.createFrom().items(one, two), context)
            .collect().asList().await().atMost(Duration.ofSeconds(2));
        assertEquals("entity-900:0", expanded.get(0).idempotencyKey());
        assertEquals("entity-900:1", expanded.get(1).idempotencyKey());
    }

    @Test
    void collectListSinkAdapterHonorsOverflowPolicies() {
        FunctionTransportContext context = FunctionTransportContext.of("req-811", "search-handler", "egress");
        Multi<TraceEnvelope<Integer>> items = Multi.createFrom().items(
            TraceEnvelope.root("trace-sink", "item-1", "search.token", "v1", "idem-1", 1),
            TraceEnvelope.root("trace-sink", "item-2", "search.token", "v1", "idem-2", 2),
            TraceEnvelope.root("trace-sink", "item-3", "search.token", "v1", "idem-3", 3));

        CollectListFunctionSinkAdapter<Integer> dropSink = new CollectListFunctionSinkAdapter<>(
            new BatchingPolicy(2, 1024, Duration.ofMillis(50), 1, BatchOverflowPolicy.DROP));
        List<Integer> dropped = dropSink.emitMany(items, context).await().atMost(Duration.ofSeconds(2));
        assertEquals(List.of(1, 2), dropped);

        CollectListFunctionSinkAdapter<Integer> failSink = new CollectListFunctionSinkAdapter<>(
            new BatchingPolicy(2, 1024, Duration.ofMillis(50), 1, BatchOverflowPolicy.FAIL));
        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> failSink.emitMany(Multi.createFrom().items(
                TraceEnvelope.root("trace-sink", "item-1", "search.token", "v1", "idem-1", 1),
                TraceEnvelope.root("trace-sink", "item-2", "search.token", "v1", "idem-2", 2),
                TraceEnvelope.root("trace-sink", "item-3", "search.token", "v1", "idem-3", 3)), context)
                .await().atMost(Duration.ofSeconds(2)));
        assertEquals("Function sink overflow: received 3 items with maxItems=2 and overflowPolicy=FAIL", ex.getMessage());
    }
}
