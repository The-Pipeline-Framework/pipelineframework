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

import jakarta.ws.rs.ConstrainedTo;
import jakarta.ws.rs.RuntimeType;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.ext.Provider;

import io.quarkus.arc.Unremovable;
import java.util.Objects;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

/**
 * Propagates pipeline context headers on REST client requests.
 */
@Provider
@ConstrainedTo(RuntimeType.CLIENT)
@Unremovable
public class PipelineContextClientRequestFilter implements ClientRequestFilter {

    /**
     * Creates a new PipelineContextClientRequestFilter.
     */
    public PipelineContextClientRequestFilter() {
    }

    /**
     * Propagates the current pipeline context and transport dispatch metadata into the outgoing client request headers.
     *
     * If a PipelineContext is available it sets VERSION, REPLAY, and CACHE_POLICY headers on the provided request.
     * If TransportDispatchMetadata is available it also sets TPF_CORRELATION_ID, TPF_EXECUTION_ID,
     * TPF_IDEMPOTENCY_KEY, TPF_RETRY_ATTEMPT, TPF_DEADLINE_EPOCH_MS, TPF_DISPATCH_TS_EPOCH_MS, and TPF_PARENT_ITEM_ID.
     *
     * @param requestContext the outgoing client request context to which headers will be added
     */
    @Override
    public void filter(ClientRequestContext requestContext) {
        PipelineContext context = PipelineContextHolder.get();
        if (context == null) {
            return;
        }
        putIfPresent(requestContext, PipelineContextHeaders.VERSION, context.versionTag());
        putIfPresent(requestContext, PipelineContextHeaders.REPLAY, context.replayMode());
        putIfPresent(requestContext, PipelineContextHeaders.CACHE_POLICY, context.cachePolicy());
        TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
        if (metadata != null) {
            putIfPresent(requestContext, PipelineContextHeaders.TPF_CORRELATION_ID, metadata.correlationId());
            putIfPresent(requestContext, PipelineContextHeaders.TPF_EXECUTION_ID, metadata.executionId());
            putIfPresent(requestContext, PipelineContextHeaders.TPF_IDEMPOTENCY_KEY, metadata.idempotencyKey());
            putIfPresent(requestContext, PipelineContextHeaders.TPF_RETRY_ATTEMPT, toStringValue(metadata.retryAttempt()));
            putIfPresent(requestContext, PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS, toStringValue(metadata.deadlineEpochMs()));
            putIfPresent(requestContext, PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS, toStringValue(metadata.dispatchTsEpochMs()));
            putIfPresent(requestContext, PipelineContextHeaders.TPF_PARENT_ITEM_ID, metadata.parentItemId());
        }
    }

    /**
     * Adds a single header to the outgoing client request if the provided value is non-null and not blank.
     *
     * @param requestContext the client request context to modify
     * @param name the header name to set
     * @param value the header value; the header is added only when this value is not null and contains non-whitespace characters
     */
    private void putIfPresent(ClientRequestContext requestContext, String name, String value) {
        if (value != null && !value.isBlank()) {
            requestContext.getHeaders().putSingle(name, value);
        }
    }

    /**
     * Convert an object to its string representation, returning null for a null input.
     *
     * @param value the object to convert
     * @return the object's String representation, or {@code null} if {@code value} is {@code null}
     */
    private String toStringValue(Object value) {
        return Objects.toString(value, null);
    }
}
