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

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * JSON codec for the TPF compatibility envelope.
 */
public final class TpfEnvelopeCodec {
    private final ObjectMapper mapper;

    public TpfEnvelopeCodec() {
        this(PipelineJson.mapper());
    }

    public TpfEnvelopeCodec(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    }

    public TpfEnvelope envelope(TpfEnvelopeControl control, TpfEnvelopePayload payload) {
        return new TpfEnvelope(TpfEnvelope.PROTOCOL_VERSION, control, payload);
    }

    public TpfEnvelopePayload jsonPayload(Object value, String schemaHint) {
        Objects.requireNonNull(value, "value must not be null");
        return new TpfEnvelopePayload(
            "json",
            "application/json",
            schemaHint,
            "utf-8",
            mapper.valueToTree(value),
            Map.of());
    }

    public TpfEnvelopePayload bytesPayload(byte[] value, String contentType, String schemaHint) {
        Objects.requireNonNull(value, "value must not be null");
        ObjectNode data = mapper.createObjectNode();
        data.put("bytesBase64", Base64.getEncoder().encodeToString(value));
        String normalizedContentType = contentType == null ? null : contentType.trim();
        return new TpfEnvelopePayload(
            "bytes",
            normalizedContentType == null || normalizedContentType.isBlank()
                ? "application/octet-stream"
                : normalizedContentType,
            schemaHint,
            "base64",
            data,
            Map.of());
    }

    public TpfEnvelopePayload refPayload(TpfPayloadRef ref, String schemaHint) {
        Objects.requireNonNull(ref, "ref must not be null");
        return new TpfEnvelopePayload(
            "ref",
            ref.contentType(),
            schemaHint,
            "ref",
            mapper.valueToTree(ref),
            Map.of());
    }

    public byte[] write(TpfEnvelope envelope) {
        Objects.requireNonNull(envelope, "envelope must not be null");
        try {
            return mapper.writeValueAsBytes(envelope);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode TPF envelope", e);
        }
    }

    public String writeString(TpfEnvelope envelope) {
        Objects.requireNonNull(envelope, "envelope must not be null");
        try {
            return mapper.writeValueAsString(envelope);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode TPF envelope", e);
        }
    }

    public TpfEnvelope read(byte[] body) {
        Objects.requireNonNull(body, "body must not be null");
        try {
            return mapper.readValue(body, TpfEnvelope.class);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode TPF envelope", e);
        }
    }

    public TpfEnvelope read(String body) {
        Objects.requireNonNull(body, "body must not be null");
        try {
            return mapper.readValue(body, TpfEnvelope.class);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode TPF envelope", e);
        }
    }

    public <T> T readJsonPayload(TpfEnvelopePayload payload, Class<T> targetType) {
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(targetType, "targetType must not be null");
        if (!"json".equalsIgnoreCase(payload.kind())) {
            throw new IllegalArgumentException("Expected json envelope payload but got '" + payload.kind() + "'");
        }
        try {
            return mapper.treeToValue(payload.data(), targetType);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode TPF envelope JSON payload as " + targetType.getName(), e);
        }
    }

    public byte[] readBytesPayload(TpfEnvelopePayload payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        if (!"bytes".equalsIgnoreCase(payload.kind())) {
            throw new IllegalArgumentException("Expected bytes envelope payload but got '" + payload.kind() + "'");
        }
        String encoded = payload.data().path("bytesBase64").asText();
        if (encoded.isBlank()) {
            throw new IllegalArgumentException("bytes envelope payload requires data.bytesBase64");
        }
        return Base64.getDecoder().decode(encoded);
    }

    public TpfPayloadRef readRefPayload(TpfEnvelopePayload payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        if (!"ref".equalsIgnoreCase(payload.kind())) {
            throw new IllegalArgumentException("Expected ref envelope payload but got '" + payload.kind() + "'");
        }
        try {
            return mapper.treeToValue(payload.data(), TpfPayloadRef.class);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode TPF envelope reference payload", e);
        }
    }
}
