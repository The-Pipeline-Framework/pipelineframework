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

package org.pipelineframework.envelope;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpfEnvelopeCodecTest {

    private final TpfEnvelopeCodec codec = new TpfEnvelopeCodec();

    @Test
    void roundTripsJsonPayloadWithStrictControlMetadata() {
        TpfEnvelopeControl control = control(Map.of("tenant", "tenant-a"));
        TpfEnvelope envelope = codec.envelope(control, codec.jsonPayload(new DemoPayload("doc-1", 3), DemoPayload.class.getName()));

        TpfEnvelope decoded = codec.read(codec.write(envelope));
        DemoPayload payload = codec.readJsonPayload(decoded.payload(), DemoPayload.class);

        assertEquals(TpfEnvelope.PROTOCOL_VERSION, decoded.protocolVersion());
        assertEquals("Chunk", decoded.control().step());
        assertEquals("chunker", decoded.control().operatorId());
        assertEquals("corr-1", decoded.control().context().get("correlationId"));
        assertEquals("tenant-a", decoded.control().metadata().get("tenant"));
        assertEquals(new DemoPayload("doc-1", 3), payload);
    }

    @Test
    void normalizesNullControlAndPayloadMetadataToImmutableEmptyMaps() {
        TpfEnvelopeControl control = control(null);
        TpfEnvelopePayload payload = codec.jsonPayload(new DemoPayload("doc-2", 1), DemoPayload.class.getName());

        assertTrue(control.metadata().isEmpty());
        assertTrue(payload.metadata().isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> control.metadata().put("x", "y"));
        assertThrows(UnsupportedOperationException.class, () -> payload.metadata().put("x", "y"));
    }

    @Test
    void supportsBytesPayload() {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        TpfEnvelopePayload payload = codec.bytesPayload(bytes, "text/plain", "raw-text");

        TpfEnvelope decoded = codec.read(codec.write(codec.envelope(control(Map.of()), payload)));

        assertEquals("bytes", decoded.payload().kind());
        assertEquals("text/plain", decoded.payload().contentType());
        assertArrayEquals(bytes, codec.readBytesPayload(decoded.payload()));
    }

    @Test
    void supportsReferencePayload() {
        TpfPayloadRef ref = new TpfPayloadRef(" repo://documents/doc-1 ", "application/pdf",
            Map.of("sizeBytes", "1024", "sha256", "abc123"));

        TpfEnvelope decoded = codec.read(codec.write(codec.envelope(control(Map.of()), codec.refPayload(ref, "DocumentRef"))));
        TpfPayloadRef decodedRef = codec.readRefPayload(decoded.payload());

        assertEquals("ref", decoded.payload().kind());
        assertEquals("repo://documents/doc-1", decodedRef.ref());
        assertEquals("application/pdf", decodedRef.contentType());
    }

    @Test
    void rejectsUnsupportedProtocolVersion() {
        TpfEnvelope envelope = codec.envelope(control(Map.of()), codec.jsonPayload(new DemoPayload("doc-3", 1), "DemoPayload"));
        String json = codec.writeString(envelope).replace(TpfEnvelope.PROTOCOL_VERSION, "tpf.envelope.v0");

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> codec.read(json));

        assertTrue(error.getMessage().contains("Failed to decode TPF envelope"));
    }

    @Test
    void rejectsJsonReaderForNonJsonPayload() {
        TpfEnvelopePayload payload = codec.bytesPayload("hello".getBytes(StandardCharsets.UTF_8), "text/plain", "raw-text");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> codec.readJsonPayload(payload, DemoPayload.class));

        assertTrue(error.getMessage().contains("Expected json envelope payload"));
    }

    @Test
    void rejectsNullCodecInputs() {
        assertThrows(NullPointerException.class, () -> codec.write(null));
        assertThrows(NullPointerException.class, () -> codec.read((byte[]) null));
        assertThrows(NullPointerException.class, () -> codec.read((String) null));
        assertThrows(NullPointerException.class, () -> codec.jsonPayload(null, "DemoPayload"));
        assertThrows(NullPointerException.class, () -> codec.bytesPayload(null, "text/plain", "raw-text"));
        assertThrows(NullPointerException.class, () -> codec.refPayload(null, "DocumentRef"));
        assertThrows(NullPointerException.class, () -> codec.readBytesPayload(null));
        assertThrows(NullPointerException.class, () -> codec.readRefPayload(null));
    }

    @Test
    void bytesPayloadDefaultsContentType() {
        TpfEnvelopePayload payload = codec.bytesPayload("hello".getBytes(StandardCharsets.UTF_8), null, "raw-text");

        assertEquals("application/octet-stream", payload.contentType());
    }

    private TpfEnvelopeControl control(Map<String, String> metadata) {
        return new TpfEnvelopeControl(
            "pipeline-v1",
            "Chunk",
            "chunker",
            Map.of(
                "correlationId", "corr-1",
                "executionId", "exec-1",
                "idempotencyKey", "idem-1",
                "retryAttempt", "2",
                "deadlineEpochMs", "2000000000000",
                "dispatchTsEpochMs", "1900000000000",
                "parentItemId", "parent-1"),
            metadata);
    }

    private record DemoPayload(String id, int count) {
    }
}
