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
     * Create a TransportDispatchMetadata instance by normalizing and parsing header string values.
     *
     * Trims blank strings to null and parses numeric header values; numeric fields are set to
     * null when the corresponding header is blank or cannot be parsed as a number.
     *
     * @param correlationId   correlation id header (trimmed; blank -> null)
     * @param executionId     execution id header (trimmed; blank -> null)
     * @param idempotencyKey  idempotency key header (trimmed; blank -> null)
     * @param retryAttempt    retry attempt header (parsed to Integer; blank or invalid -> null)
     * @param deadlineEpochMs deadline epoch ms header (parsed to Long; blank or invalid -> null)
     * @param dispatchTsEpochMs dispatch timestamp epoch ms header (parsed to Long; blank or invalid -> null)
     * @param parentItemId    parent item id header (trimmed; blank -> null)
     * @return a TransportDispatchMetadata with normalized string fields and parsed numeric fields (or null when absent/invalid)
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
     * Normalize a header-like string by trimming surrounding whitespace and treating null or blank inputs as absent.
     *
     * @param value the input string which may be null or contain only whitespace
     * @return the trimmed string, or {@code null} if {@code value} is {@code null} or contains only whitespace
     */
    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    /**
     * Parses an Integer from a header-like string, returning null for blank or invalid input.
     *
     * @param value the raw string value to parse; may be null or blank
     * @return the parsed Integer, or {@code null} if the input is null, blank, or not a valid integer
     */
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

    /**
     * Parses a string as a Long after trimming; blank or unparsable values produce {@code null}.
     *
     * <p>The input is trimmed and treated as {@code null} if blank. If the resulting value is a
     * valid long literal it is returned, otherwise {@code null} is returned.
     *
     * @param value the string value to parse (may be null or blank)
     * @return the parsed {@link Long}, or {@code null} if the input is null, blank, or not a valid long
     */
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
