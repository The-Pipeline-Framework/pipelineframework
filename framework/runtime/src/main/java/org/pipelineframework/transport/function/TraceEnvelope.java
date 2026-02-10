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

import java.time.Instant;
import java.util.Map;

/**
 * Transport-neutral lineage envelope used by function adapters.
 *
 * @param traceId distributed trace id
 * @param spanId optional span id for the active processing scope
 * @param itemId unique id for this envelope item
 * @param previousItemRef pointer to previous item in lineage chain
 * @param payloadModel logical payload model identifier
 * @param payloadModelVersion model/schema version
 * @param idempotencyKey deterministic key for deduplication at adapter boundaries
 * @param payload domain payload
 * @param occurredAt event timestamp
 * @param meta optional metadata
 * @param <T> payload type
 */
public record TraceEnvelope<T>(
    String traceId,
    String spanId,
    String itemId,
    TraceLink previousItemRef,
    String payloadModel,
    String payloadModelVersion,
    String idempotencyKey,
    T payload,
    Instant occurredAt,
    Map<String, String> meta
) {
    private TraceEnvelope(
        String traceId,
        String spanId,
        String itemId,
        TraceLink previousItemRef,
        String payloadModel,
        String payloadModelVersion,
        String idempotencyKey,
        T payload,
        Instant occurredAt,
        Map<String, String> meta,
        boolean immutableMeta
    ) {
        this(
            traceId,
            spanId,
            itemId,
            previousItemRef,
            payloadModel,
            payloadModelVersion,
            idempotencyKey,
            payload,
            occurredAt,
            immutableMeta ? (meta == null ? Map.of() : Map.copyOf(meta)) : meta);
    }

    /**
     * Creates a validated immutable envelope.
     */
    public TraceEnvelope {
        if (traceId == null || traceId.isBlank()) {
            throw new IllegalArgumentException("traceId is required");
        }
        if (itemId == null || itemId.isBlank()) {
            throw new IllegalArgumentException("itemId is required");
        }
        if (payloadModel == null || payloadModel.isBlank()) {
            throw new IllegalArgumentException("payloadModel is required");
        }
        if (payloadModelVersion == null || payloadModelVersion.isBlank()) {
            throw new IllegalArgumentException("payloadModelVersion is required");
        }
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey is required");
        }
        if (occurredAt == null) {
            occurredAt = Instant.now();
        }
        meta = meta == null ? Map.of() : Map.copyOf(meta);
    }

    /**
     * Creates a root envelope (no previous item link).
     *
     * @param traceId trace id
     * @param itemId item id
     * @param payloadModel model id
     * @param payloadModelVersion model version
     * @param idempotencyKey idempotency key
     * @param payload payload
     * @param <T> payload type
     * @return root envelope
     */
    public static <T> TraceEnvelope<T> root(
        String traceId,
        String itemId,
        String payloadModel,
        String payloadModelVersion,
        String idempotencyKey,
        T payload
    ) {
        return new TraceEnvelope<>(
            traceId,
            null,
            itemId,
            null,
            payloadModel,
            payloadModelVersion,
            idempotencyKey,
            payload,
            Instant.now(),
            Map.of());
    }

    /**
     * Builds the next envelope in the chain from this item.
     *
     * @param newItemId new item id
     * @param newPayloadModel output model id
     * @param newPayloadModelVersion output model version
     * @param newIdempotencyKey output idempotency key
     * @param newPayload output payload
     * @param <N> output payload type
     * @return next envelope linked to the current item
     */
    public <N> TraceEnvelope<N> next(
        String newItemId,
        String newPayloadModel,
        String newPayloadModelVersion,
        String newIdempotencyKey,
        N newPayload
    ) {
        return new TraceEnvelope<>(
            traceId,
            spanId,
            newItemId,
            TraceLink.reference(itemId),
            newPayloadModel,
            newPayloadModelVersion,
            newIdempotencyKey,
            newPayload,
            Instant.now(),
            meta,
            true);
    }
}
