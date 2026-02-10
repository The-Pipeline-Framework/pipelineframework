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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
