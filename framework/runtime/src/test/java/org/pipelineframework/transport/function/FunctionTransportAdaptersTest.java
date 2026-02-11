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
}
