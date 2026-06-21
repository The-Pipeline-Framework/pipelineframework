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

import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Loose payload region inside a strict TPF envelope.
 *
 * @param kind payload variant: {@code json}, {@code bytes}, or {@code ref}
 * @param contentType payload media type
 * @param schemaHint schema or class-name hint for loose consumers
 * @param encoding payload encoding such as {@code utf-8} or {@code base64}
 * @param data variant data; JSON payload directly, bytes object, or reference object
 * @param metadata additional payload metadata
 */
public record TpfEnvelopePayload(
    String kind,
    String contentType,
    String schemaHint,
    String encoding,
    JsonNode data,
    Map<String, String> metadata
) {
    public TpfEnvelopePayload {
        kind = requireKind(kind);
        contentType = contentType == null || contentType.isBlank() ? defaultContentType(kind) : contentType.trim();
        schemaHint = schemaHint == null || schemaHint.isBlank() ? "unspecified" : schemaHint.trim();
        encoding = encoding == null || encoding.isBlank() ? defaultEncoding(kind) : encoding.trim();
        data = Objects.requireNonNull(data, "data must not be null");
        if (data.isNull()) {
            throw new IllegalArgumentException("data must not be JSON null");
        }
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    private static String requireKind(String kind) {
        if (kind == null || kind.isBlank()) {
            throw new IllegalArgumentException("kind must not be blank");
        }
        String normalized = kind.trim().toLowerCase();
        if (!"json".equals(normalized) && !"bytes".equals(normalized) && !"ref".equals(normalized)) {
            throw new IllegalArgumentException("Unsupported envelope payload kind '" + kind + "'");
        }
        return normalized;
    }

    private static String defaultContentType(String kind) {
        return "json".equals(kind) ? "application/json" : "application/octet-stream";
    }

    private static String defaultEncoding(String kind) {
        return "bytes".equals(kind) ? "base64" : "utf-8";
    }
}
