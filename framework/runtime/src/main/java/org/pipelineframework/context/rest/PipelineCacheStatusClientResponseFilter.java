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
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import jakarta.ws.rs.ext.Provider;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContextHeaders;

/**
 * Captures cache status headers from REST client responses.
 */
@Provider
@ConstrainedTo(RuntimeType.CLIENT)
@Unremovable
public class PipelineCacheStatusClientResponseFilter implements ClientResponseFilter {

    /**
     * Creates a new PipelineCacheStatusClientResponseFilter.
     */
    public PipelineCacheStatusClientResponseFilter() {
    }

    @Override
    public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) {
        if (responseContext == null || responseContext.getHeaders() == null) {
            PipelineCacheStatusHolder.clear();
            return;
        }
        String header = responseContext.getHeaderString(PipelineContextHeaders.CACHE_STATUS);
        CacheStatus status = CacheStatus.fromHeader(header);
        PipelineCacheStatusHolder.set(status);
    }
}
