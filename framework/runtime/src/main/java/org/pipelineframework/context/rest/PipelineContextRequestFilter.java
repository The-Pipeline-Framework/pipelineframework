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

import jakarta.annotation.Priority;
import jakarta.ws.rs.ConstrainedTo;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.RuntimeType;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

/**
 * Extracts pipeline context headers from REST requests.
 */
@Provider
@ConstrainedTo(RuntimeType.SERVER)
@Priority(Priorities.AUTHENTICATION)
@Unremovable
public class PipelineContextRequestFilter implements ContainerRequestFilter {

    /**
     * Creates a new PipelineContextRequestFilter.
     */
    public PipelineContextRequestFilter() {
    }

    /**
     * Extracts pipeline context and transport dispatch metadata from HTTP headers and stores them in their respective thread-local holders.
     *
     * Reads pipeline headers (version, replay, cache policy) and transport dispatch headers
     * (correlation id, execution id, idempotency key, retry attempt, deadline epoch ms,
     * dispatch timestamp epoch ms, parent item id) from the provided request context and sets
     * the resulting PipelineContext and TransportDispatchMetadata into PipelineContextHolder
     * and TransportDispatchMetadataHolder respectively.
     *
     * @param requestContext the JAX-RS request context used to read the HTTP headers
     */
    @Override
    public void filter(ContainerRequestContext requestContext) {
        PipelineContext context = PipelineContext.fromHeaders(
            requestContext.getHeaderString(PipelineContextHeaders.VERSION),
            requestContext.getHeaderString(PipelineContextHeaders.REPLAY),
            requestContext.getHeaderString(PipelineContextHeaders.CACHE_POLICY));
        PipelineContextHolder.set(context);
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            requestContext.getHeaderString(PipelineContextHeaders.TPF_CORRELATION_ID),
            requestContext.getHeaderString(PipelineContextHeaders.TPF_EXECUTION_ID),
            requestContext.getHeaderString(PipelineContextHeaders.TPF_IDEMPOTENCY_KEY),
            requestContext.getHeaderString(PipelineContextHeaders.TPF_RETRY_ATTEMPT),
            requestContext.getHeaderString(PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS),
            requestContext.getHeaderString(PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS),
            requestContext.getHeaderString(PipelineContextHeaders.TPF_PARENT_ITEM_ID));
        TransportDispatchMetadataHolder.set(metadata);
    }
}
