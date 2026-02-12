/*
 * Copyright (c) 2023-2025 Mariano Barcia
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TraceEnvelopeTest {

    @Test
    void rootEnvelopeCreatesNextWithPreviousReference() {
        TraceEnvelope<String> root = TraceEnvelope.root(
            "trace-1",
            "item-1",
            "search.raw-document",
            "v1",
            "idem-1",
            "payload-1");

        TraceEnvelope<Integer> next = root.next(
            "item-2",
            "search.parsed-document",
            "v2",
            "idem-2",
            42);

        assertNull(root.previousItemRef());
        assertEquals("trace-1", next.traceId());
        assertNotNull(next.previousItemRef());
        assertEquals(TraceLineageMode.REFERENCE, next.previousItemRef().mode());
        assertEquals("item-1", next.previousItemRef().previousItemId());
        assertEquals(42, next.payload());
        assertNotNull(next.occurredAt());
        assertEquals("search.parsed-document", next.payloadModel());
        assertEquals("v2", next.payloadModelVersion());
    }

    @Test
    void envelopeRequiresTraceId() {
        assertThrows(IllegalArgumentException.class, () ->
            TraceEnvelope.root(null, "item-1", "model", "v1", "idem-1", "payload"));
    }

    @Test
    void envelopeRequiresItemId() {
        assertThrows(IllegalArgumentException.class, () ->
            TraceEnvelope.root("trace-1", " ", "model", "v1", "idem-1", "payload"));
    }

    @Test
    void envelopeRequiresPayloadModel() {
        assertThrows(IllegalArgumentException.class, () ->
            TraceEnvelope.root("trace-1", "item-1", "", "v1", "idem-1", "payload"));
    }

    @Test
    void envelopeRequiresPayloadModelVersion() {
        assertThrows(IllegalArgumentException.class, () ->
            TraceEnvelope.root("trace-1", "item-1", "model", null, "idem-1", "payload"));
    }

    @Test
    void envelopeRequiresIdempotencyKey() {
        assertThrows(IllegalArgumentException.class, () ->
            TraceEnvelope.root("trace-1", "item-1", "model", "v1", " ", "payload"));
    }

    @Test
    void envelopeMetaIsImmutableFromInputMap() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("tenant", "acme");
        TraceEnvelope<String> envelope = new TraceEnvelope<>(
            "trace-1",
            null,
            "item-1",
            null,
            "model",
            "v1",
            "idem-1",
            "payload",
            null,
            metadata);
        metadata.put("tenant", "other");

        assertEquals("acme", envelope.meta().get("tenant"));
    }

    @Test
    void fanoutOneToManyKeepsTraceAndLineageStable() {
        TraceEnvelope<String> input = new TraceEnvelope<>(
            "trace-fanout-1",
            "span-a",
            "raw-1",
            null,
            "search.raw-document",
            "v1",
            "idem-raw-1",
            "raw-payload",
            null,
            Map.of("tenant", "acme", "workflow", "checkout"));

        List<TraceEnvelope<String>> fanout = Multi.createFrom().item(input)
            .onItem().transformToMulti(envelope -> Multi.createFrom().items(
                envelope.next("parsed-1-0", "search.parsed-document", "v1", envelope.idempotencyKey() + ":0", "part-0"),
                envelope.next("parsed-1-1", "search.parsed-document", "v1", envelope.idempotencyKey() + ":1", "part-1")))
            .concatenate()
            .collect().asList()
            .await().indefinitely();

        assertEquals(2, fanout.size());

        TraceEnvelope<String> first = fanout.getFirst();
        TraceEnvelope<String> second = fanout.get(1);

        assertEquals("trace-fanout-1", first.traceId());
        assertEquals("trace-fanout-1", second.traceId());
        assertEquals("raw-1", first.previousItemRef().previousItemId());
        assertEquals("raw-1", second.previousItemRef().previousItemId());
        assertEquals(TraceLineageMode.REFERENCE, first.previousItemRef().mode());
        assertEquals(TraceLineageMode.REFERENCE, second.previousItemRef().mode());
        assertEquals("acme", first.meta().get("tenant"));
        assertEquals("acme", second.meta().get("tenant"));
        assertEquals("idem-raw-1:0", first.idempotencyKey());
        assertEquals("idem-raw-1:1", second.idempotencyKey());
    }

    @Test
    void faninManyToOneUsesStableIdempotencyAndConsistentTrace() {
        TraceEnvelope<String> root = new TraceEnvelope<>(
            "trace-fanin-1",
            "span-root",
            "raw-merge-root",
            null,
            "search.raw-document",
            "v1",
            "idem-root",
            "raw",
            null,
            Map.of("tenant", "acme", "workflow", "checkout"));

        TraceEnvelope<String> left = root.next(
            "token-1",
            "search.token",
            "v1",
            "idem-root:left",
            "token-left");
        TraceEnvelope<String> right = root.next(
            "token-2",
            "search.token",
            "v1",
            "idem-root:right",
            "token-right");

        List<TraceEnvelope<String>> parts = Multi.createFrom().items(left, right)
            .collect().asList()
            .await().indefinitely();

        String stableMergeKey = "merge:" + parts.stream()
            .map(TraceEnvelope::idempotencyKey)
            .sorted()
            .collect(Collectors.joining("|"));

        TraceEnvelope<String> merged = parts.getFirst().next(
            "token-batch-1",
            "search.token-batch",
            "v1",
            stableMergeKey,
            "token-left,token-right");

        assertEquals("trace-fanin-1", merged.traceId());
        assertEquals("token-1", merged.previousItemRef().previousItemId());
        assertEquals("acme", merged.meta().get("tenant"));
        assertEquals(TraceLineageMode.REFERENCE, merged.previousItemRef().mode());
        assertEquals("merge:idem-root:left|idem-root:right", merged.idempotencyKey());
        assertIterableEquals(
            List.of("idem-root:left", "idem-root:right"),
            parts.stream().map(TraceEnvelope::idempotencyKey).sorted().toList());
    }
}
