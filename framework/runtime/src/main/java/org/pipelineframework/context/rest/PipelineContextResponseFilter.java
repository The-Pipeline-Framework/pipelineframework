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
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;

/**
 * Clears pipeline context after REST responses are processed.
 */
@Provider
@ConstrainedTo(RuntimeType.SERVER)
@Unremovable
public class PipelineContextResponseFilter implements ContainerResponseFilter {

    /**
     * Creates a new PipelineContextResponseFilter.
     */
    public PipelineContextResponseFilter() {
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        CacheStatus status = PipelineCacheStatusHolder.getAndClear();
        if (status != null) {
            responseContext.getHeaders().putSingle(PipelineContextHeaders.CACHE_STATUS, status.name());
        }
        PipelineContextHolder.clear();
    }
}
