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

package org.pipelineframework.context.rest;

import java.util.List;
import java.util.Objects;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.MultivaluedMap;

import io.quarkus.arc.Unremovable;
import org.eclipse.microprofile.rest.client.ext.ClientHeadersFactory;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

/**
 * Propagates pipeline context headers on REST client requests.
 */
@ApplicationScoped
@Unremovable
public class PipelineContextClientHeadersFactory implements ClientHeadersFactory {

    /**
     * Creates a PipelineContextClientHeadersFactory used to propagate pipeline context headers to outgoing REST client requests.
     *
     * This constructor performs no side effects during initialization.
     */
    public PipelineContextClientHeadersFactory() {
    }

    /**
     * Propagates pipeline context headers (VERSION, REPLAY, CACHE_POLICY) from incoming request headers
     * to the provided client outgoing headers, using the current PipelineContext as a fallback.
     *
     * If a header is present (case-insensitive) in incomingHeaders its value is forwarded; if absent,
     * the corresponding value is taken from PipelineContextHolder.get() when available. Only non-blank
     * values are written into clientOutgoingHeaders.
     *
     * @param incomingHeaders      the incoming request headers to read values from; may be null
     * @param clientOutgoingHeaders the outgoing client headers to update; if null, it is returned unchanged
     * @return the clientOutgoingHeaders map after any propagated headers have been set, or null if
     *         clientOutgoingHeaders was null
     */
    @Override
    public MultivaluedMap<String, String> update(
            MultivaluedMap<String, String> incomingHeaders,
            MultivaluedMap<String, String> clientOutgoingHeaders) {
        if (clientOutgoingHeaders == null) {
            return clientOutgoingHeaders;
        }
        String version = headerValue(incomingHeaders, PipelineContextHeaders.VERSION);
        String replay = headerValue(incomingHeaders, PipelineContextHeaders.REPLAY);
        String cachePolicy = headerValue(incomingHeaders, PipelineContextHeaders.CACHE_POLICY);
        String correlationId = headerValue(incomingHeaders, PipelineContextHeaders.TPF_CORRELATION_ID);
        String executionId = headerValue(incomingHeaders, PipelineContextHeaders.TPF_EXECUTION_ID);
        String idempotencyKey = headerValue(incomingHeaders, PipelineContextHeaders.TPF_IDEMPOTENCY_KEY);
        String retryAttempt = headerValue(incomingHeaders, PipelineContextHeaders.TPF_RETRY_ATTEMPT);
        String deadlineEpochMs = headerValue(incomingHeaders, PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS);
        String dispatchTsEpochMs = headerValue(incomingHeaders, PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS);
        String parentItemId = headerValue(incomingHeaders, PipelineContextHeaders.TPF_PARENT_ITEM_ID);

        if (version == null || replay == null || cachePolicy == null) {
            PipelineContext context = PipelineContextHolder.get();
            if (context != null) {
                if (version == null) {
                    version = context.versionTag();
                }
                if (replay == null) {
                    replay = context.replayMode();
                }
                if (cachePolicy == null) {
                    cachePolicy = context.cachePolicy();
                }
            }
        }
        if (correlationId == null || executionId == null || idempotencyKey == null
            || retryAttempt == null || deadlineEpochMs == null || dispatchTsEpochMs == null
            || parentItemId == null) {
            TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
            if (metadata != null) {
                if (correlationId == null) {
                    correlationId = metadata.correlationId();
                }
                if (executionId == null) {
                    executionId = metadata.executionId();
                }
                if (idempotencyKey == null) {
                    idempotencyKey = metadata.idempotencyKey();
                }
                if (retryAttempt == null) {
                    retryAttempt = toStringValue(metadata.retryAttempt());
                }
                if (deadlineEpochMs == null) {
                    deadlineEpochMs = toStringValue(metadata.deadlineEpochMs());
                }
                if (dispatchTsEpochMs == null) {
                    dispatchTsEpochMs = toStringValue(metadata.dispatchTsEpochMs());
                }
                if (parentItemId == null) {
                    parentItemId = metadata.parentItemId();
                }
            }
        }

        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.VERSION, version);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.REPLAY, replay);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.CACHE_POLICY, cachePolicy);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.TPF_CORRELATION_ID, correlationId);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.TPF_EXECUTION_ID, executionId);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.TPF_IDEMPOTENCY_KEY, idempotencyKey);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.TPF_RETRY_ATTEMPT, retryAttempt);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS, deadlineEpochMs);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS, dispatchTsEpochMs);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.TPF_PARENT_ITEM_ID, parentItemId);
        return clientOutgoingHeaders;
    }

    /**
     * Retrieve the first non-blank value for the given header name from the provided headers,
     * matching the header name case-insensitively.
     *
     * @param headers the map of header names to values; may be null
     * @param name the header name to look up; may be null
     * @return the first non-blank value for the header if found, otherwise null
     */
    private String headerValue(MultivaluedMap<String, String> headers, String name) {
        if (headers == null || name == null) {
            return null;
        }
        List<String> directValues = headers.get(name);
        if (directValues != null) {
            for (String candidate : directValues) {
                if (candidate != null && !candidate.isBlank()) {
                    return candidate;
                }
            }
        }
        for (String key : headers.keySet()) {
            if (key != null && key.equalsIgnoreCase(name)) {
                List<String> values = headers.get(key);
                if (values != null) {
                    for (String candidate : values) {
                        if (candidate != null && !candidate.isBlank()) {
                            return candidate;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Sets the header in the provided map only when the given value is non-null and contains
     * at least one non-whitespace character.
     *
     * @param headers the headers map to modify
     * @param name the header name to set
     * @param value the header value to write when present (ignored if null or only whitespace)
     */
    private void putIfPresent(
            MultivaluedMap<String, String> headers,
            String name,
            String value) {
        if (value != null && !value.isBlank()) {
            headers.putSingle(name, value);
        }
    }

    private String toStringValue(Object value) {
        return Objects.toString(value, null);
    }
}
