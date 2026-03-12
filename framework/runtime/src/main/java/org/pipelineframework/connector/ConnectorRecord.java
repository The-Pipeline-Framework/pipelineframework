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

package org.pipelineframework.connector;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.pipelineframework.context.TransportDispatchMetadata;

/**
 * Canonical connector payload plus transport metadata and optional lineage/context meta.
 *
 * @param payload mapped payload forwarded to the connector target
 * @param dispatchMetadata canonical transport metadata
 * @param meta optional lineage or connector-local metadata
 * @param <T> payload type
 */
public record ConnectorRecord<T>(
    T payload,
    TransportDispatchMetadata dispatchMetadata,
    Map<String, String> meta
) {

    public ConnectorRecord {
        meta = normalizeMeta(meta);
    }

    public static <T> ConnectorRecord<T> ofPayload(T payload) {
        return new ConnectorRecord<>(payload, null, Map.of());
    }

    public static <T> ConnectorRecord<T> ofPayload(T payload, TransportDispatchMetadata dispatchMetadata) {
        return new ConnectorRecord<>(payload, dispatchMetadata, Map.of());
    }

    public static <T> ConnectorRecord<T> ofPayload(
        T payload,
        TransportDispatchMetadata dispatchMetadata,
        Map<String, String> meta
    ) {
        return new ConnectorRecord<>(payload, dispatchMetadata, meta);
    }

    public String idempotencyKey() {
        return dispatchMetadata == null ? null : dispatchMetadata.idempotencyKey();
    }

    public ConnectorRecord<T> withDispatchMetadata(TransportDispatchMetadata metadata) {
        return new ConnectorRecord<>(payload, metadata, meta);
    }

    public <N> ConnectorRecord<N> withPayload(N nextPayload) {
        return new ConnectorRecord<>(nextPayload, dispatchMetadata, meta);
    }

    public ConnectorRecord<T> withMeta(Map<String, String> nextMeta) {
        return new ConnectorRecord<>(payload, dispatchMetadata, nextMeta);
    }

    /**
     * Returns a copy of this record with one additional metadata entry.
     *
     * @param key metadata key; must not be null
     * @param value metadata value; must not be null
     * @return copied record with the extra metadata entry
     * @throws NullPointerException if {@code key} or {@code value} is null
     */
    public ConnectorRecord<T> withMetaEntry(String key, String value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        Map<String, String> updated = new LinkedHashMap<>(meta);
        updated.put(key, value);
        return new ConnectorRecord<>(payload, dispatchMetadata, updated);
    }

    private static Map<String, String> normalizeMeta(Map<String, String> meta) {
        if (meta == null || meta.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(meta));
    }
}
