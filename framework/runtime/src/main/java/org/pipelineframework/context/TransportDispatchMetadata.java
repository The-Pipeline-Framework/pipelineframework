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

package org.pipelineframework.context;

import org.jboss.logging.Logger;

/**
 * Canonical transport metadata used for replay-safe and duplicate-safe dispatch across transports.
 *
 * @param correlationId immutable end-to-end correlation identifier
 * @param executionId orchestrator execution instance identifier
 * @param idempotencyKey stable idempotency token for duplicate suppression
 * @param retryAttempt retry attempt number (0-based)
 * @param deadlineEpochMs absolute deadline in epoch milliseconds
 * @param dispatchTsEpochMs dispatch timestamp in epoch milliseconds
 * @param parentItemId optional lineage parent item identifier
 */
public record TransportDispatchMetadata(
    String correlationId,
    String executionId,
    String idempotencyKey,
    Integer retryAttempt,
    Long deadlineEpochMs,
    Long dispatchTsEpochMs,
    String parentItemId
) {
    private static final Logger LOG = Logger.getLogger(TransportDispatchMetadata.class);

    public TransportDispatchMetadata {
        if (retryAttempt != null && retryAttempt < 0) {
            throw new IllegalArgumentException("retryAttempt must be >= 0");
        }
        if (deadlineEpochMs != null && deadlineEpochMs < 0) {
            throw new IllegalArgumentException("deadlineEpochMs must be >= 0");
        }
        if (dispatchTsEpochMs != null && dispatchTsEpochMs < 0) {
            throw new IllegalArgumentException("dispatchTsEpochMs must be >= 0");
        }
    }


    /**
     * Builds a TransportDispatchMetadata instance from header values, normalizing blank strings to `null`
     * and parsing numeric header strings to their numeric types.
     *
     * @param correlationId correlation id header; trimmed to `null` if blank
     * @param executionId execution id header; trimmed to `null` if blank
     * @param idempotencyKey idempotency key header; trimmed to `null` if blank
     * @param retryAttempt retry attempt header as a string; parsed to an `Integer` if present and valid
     * @param deadlineEpochMs deadline header as a string; parsed to a `Long` if present and valid
     * @param dispatchTsEpochMs dispatch timestamp header as a string; parsed to a `Long` if present and valid
     * @param parentItemId parent item header; trimmed to `null` if blank
     * @return a TransportDispatchMetadata with normalized string fields and parsed numeric fields where applicable
     */
    public static TransportDispatchMetadata fromHeaders(
        String correlationId,
        String executionId,
        String idempotencyKey,
        String retryAttempt,
        String deadlineEpochMs,
        String dispatchTsEpochMs,
        String parentItemId
    ) {
        return new TransportDispatchMetadata(
            normalize(correlationId),
            normalize(executionId),
            normalize(idempotencyKey),
            parseInt(retryAttempt),
            parseLong(deadlineEpochMs),
            parseLong(dispatchTsEpochMs),
            normalize(parentItemId));
    }

    /**
     * Create a copy of this metadata with the specified idempotency key.
     *
     * @param nextIdempotencyKey the idempotency key to set on the returned metadata
     * @return a new TransportDispatchMetadata identical to this instance except with the specified idempotency key
     */
    public TransportDispatchMetadata withIdempotencyKey(String nextIdempotencyKey) {
        return new TransportDispatchMetadata(
            correlationId,
            executionId,
            normalize(nextIdempotencyKey),
            retryAttempt,
            deadlineEpochMs,
            dispatchTsEpochMs,
            parentItemId);
    }

    /**
     * Converts an input string to a trimmed value or yields no value for blank input.
     *
     * @param value the input string to normalize
     * @return the input string with leading and trailing whitespace removed, or `null` if the input is `null` or contains only whitespace
     */
    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static Integer parseInt(String value) {
        String normalized = normalize(value);
        if (normalized == null) {
            return null;
        }
        try {
            return Integer.parseInt(normalized);
        } catch (NumberFormatException e) {
            LOG.debugf(e, "Ignoring invalid integer metadata value '%s'", normalized);
            return null;
        }
    }

    private static Long parseLong(String value) {
        String normalized = normalize(value);
        if (normalized == null) {
            return null;
        }
        try {
            return Long.parseLong(normalized);
        } catch (NumberFormatException e) {
            LOG.debugf(e, "Ignoring invalid long metadata value '%s'", normalized);
            return null;
        }
    }
}
