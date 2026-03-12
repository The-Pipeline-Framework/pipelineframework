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

    /**
     * Create a ConnectorRecord containing the given payload with no dispatch metadata and an empty metadata map.
     *
     * @param  payload the payload to include in the record
     * @return         a ConnectorRecord with the provided payload, `null` dispatch metadata, and an empty meta map
     */
    public static <T> ConnectorRecord<T> ofPayload(T payload) {
        return new ConnectorRecord<>(payload, null, Map.of());
    }

    /**
     * Creates a ConnectorRecord containing the given payload and dispatch metadata with an empty metadata map.
     *
     * @param <T> the payload type
     * @param payload the payload to include in the record
     * @param dispatchMetadata canonical transport dispatch metadata; may be null
     * @return a ConnectorRecord holding the provided payload and dispatch metadata and an empty immutable meta map
     */
    public static <T> ConnectorRecord<T> ofPayload(T payload, TransportDispatchMetadata dispatchMetadata) {
        return new ConnectorRecord<>(payload, dispatchMetadata, Map.of());
    }

    /**
     * Create a ConnectorRecord containing the given payload, dispatch metadata, and an immutable normalized meta map.
     *
     * The `meta` parameter is normalized: if `null` or empty an empty immutable map is used; otherwise an unmodifiable
     * LinkedHashMap copy is created to preserve insertion order.
     *
     * @param payload           the payload forwarded to the connector target
     * @param dispatchMetadata  canonical transport dispatch metadata; may be null
     * @param meta              optional metadata map to include with the record; will be normalized as described above
     * @return                  a ConnectorRecord with the provided payload, dispatch metadata, and normalized meta
     */
    public static <T> ConnectorRecord<T> ofPayload(
        T payload,
        TransportDispatchMetadata dispatchMetadata,
        Map<String, String> meta
    ) {
        return new ConnectorRecord<>(payload, dispatchMetadata, meta);
    }

    /**
     * Retrieve the idempotency key associated with this record's dispatch metadata.
     *
     * @return the idempotency key from the dispatch metadata, or `null` if no dispatch metadata is present
     */
    public String idempotencyKey() {
        return dispatchMetadata == null ? null : dispatchMetadata.idempotencyKey();
    }

    /**
     * Create a new ConnectorRecord with the given transport dispatch metadata while preserving the payload and meta.
     *
     * @param metadata the transport dispatch metadata to set; may be {@code null} to clear dispatch metadata
     * @return a ConnectorRecord with the provided dispatch metadata and the original payload and meta
     */
    public ConnectorRecord<T> withDispatchMetadata(TransportDispatchMetadata metadata) {
        return new ConnectorRecord<>(payload, metadata, meta);
    }

    /**
     * Create a new ConnectorRecord with the given payload while preserving this record's dispatch metadata and meta.
     *
     * @param nextPayload the payload for the new record
     * @return            a ConnectorRecord containing the provided payload and the original dispatch metadata and meta
     */
    public <N> ConnectorRecord<N> withPayload(N nextPayload) {
        return new ConnectorRecord<>(nextPayload, dispatchMetadata, meta);
    }

    /**
     * Create a new ConnectorRecord preserving payload and dispatch metadata while replacing the meta map.
     *
     * @param nextMeta the meta map to apply; may be null or empty — it will be normalized to an immutable empty map or an unmodifiable insertion-ordered copy
     * @return a ConnectorRecord with the same payload and dispatchMetadata and the provided meta (after normalization)
     */
    public ConnectorRecord<T> withMeta(Map<String, String> nextMeta) {
        return new ConnectorRecord<>(payload, dispatchMetadata, nextMeta);
    }

    /**
     * Creates a new ConnectorRecord whose meta includes the given key and value.
     *
     * @param key metadata key; must not be null
     * @param value metadata value; must not be null
     * @return the new ConnectorRecord with the same payload and dispatch metadata, and meta augmented by the given entry
     * @throws NullPointerException if {@code key} or {@code value} is null
     */
    public ConnectorRecord<T> withMetaEntry(String key, String value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        Map<String, String> updated = new LinkedHashMap<>(meta);
        updated.put(key, value);
        return new ConnectorRecord<>(payload, dispatchMetadata, updated);
    }

    /**
     * Normalize a metadata map into a stable, immutable form.
     *
     * If the input is null or empty, an empty immutable map is returned.
     * Otherwise a new unmodifiable LinkedHashMap copy is returned to preserve
     * insertion order while preventing modification.
     *
     * @param meta the metadata map to normalize; may be null
     * @return an immutable map containing the same entries (empty if input was null or empty)
     */
    private static Map<String, String> normalizeMeta(Map<String, String> meta) {
        if (meta == null || meta.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(meta));
    }
}
