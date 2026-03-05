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

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class PipelineContextResponseFilterTest {

    @AfterEach
    void cleanup() {
        PipelineContextHolder.clear();
        TransportDispatchMetadataHolder.clear();
        PipelineCacheStatusHolder.clear();
    }

    @Test
    void clearsPipelineContextAfterResponse() {
        PipelineContextResponseFilter filter = new PipelineContextResponseFilter();
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        ContainerResponseContext responseContext = createMockResponseContext();

        PipelineContextHolder.set(new PipelineContext("v1", "true", "prefer-cache"));

        filter.filter(requestContext, responseContext);

        assertNull(PipelineContextHolder.get());
    }

    @Test
    void clearsTransportDispatchMetadataAfterResponse() {
        PipelineContextResponseFilter filter = new PipelineContextResponseFilter();
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        ContainerResponseContext responseContext = createMockResponseContext();

        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-123", "exec-456", "idem-789", 0, null, null, null));

        filter.filter(requestContext, responseContext);

        assertNull(TransportDispatchMetadataHolder.get());
    }

    @Test
    void propagatesCacheStatusToResponseHeader() {
        PipelineContextResponseFilter filter = new PipelineContextResponseFilter();
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        ContainerResponseContext responseContext = createMockResponseContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(responseContext.getHeaders()).thenReturn(headers);

        PipelineCacheStatusHolder.set(CacheStatus.HIT);

        filter.filter(requestContext, responseContext);

        assertEquals("HIT", headers.getFirst(PipelineContextHeaders.CACHE_STATUS));
        assertNull(PipelineCacheStatusHolder.get());
    }

    @Test
    void doesNotAddCacheStatusHeaderWhenStatusIsNull() {
        PipelineContextResponseFilter filter = new PipelineContextResponseFilter();
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        ContainerResponseContext responseContext = createMockResponseContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(responseContext.getHeaders()).thenReturn(headers);

        filter.filter(requestContext, responseContext);

        assertNull(headers.getFirst(PipelineContextHeaders.CACHE_STATUS));
    }

    @Test
    void clearsCacheStatusAfterResponse() {
        PipelineContextResponseFilter filter = new PipelineContextResponseFilter();
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        ContainerResponseContext responseContext = createMockResponseContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(responseContext.getHeaders()).thenReturn(headers);

        PipelineCacheStatusHolder.set(CacheStatus.MISS);

        filter.filter(requestContext, responseContext);

        assertNull(PipelineCacheStatusHolder.get());
    }

    @Test
    void handlesDifferentCacheStatusValues() {
        PipelineContextResponseFilter filter = new PipelineContextResponseFilter();
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        ContainerResponseContext responseContext = createMockResponseContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(responseContext.getHeaders()).thenReturn(headers);

        PipelineCacheStatusHolder.set(CacheStatus.BYPASS);

        filter.filter(requestContext, responseContext);

        assertEquals("BYPASS", headers.getFirst(PipelineContextHeaders.CACHE_STATUS));
    }

    @Test
    void clearsBothContextAndMetadata() {
        PipelineContextResponseFilter filter = new PipelineContextResponseFilter();
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        ContainerResponseContext responseContext = createMockResponseContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(responseContext.getHeaders()).thenReturn(headers);

        PipelineContextHolder.set(new PipelineContext("v1", "true", "prefer-cache"));
        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-123", "exec-456", "idem-789", 0, null, null, null));
        PipelineCacheStatusHolder.set(CacheStatus.HIT);

        filter.filter(requestContext, responseContext);

        assertNull(PipelineContextHolder.get());
        assertNull(TransportDispatchMetadataHolder.get());
        assertNull(PipelineCacheStatusHolder.get());
        assertEquals("HIT", headers.getFirst(PipelineContextHeaders.CACHE_STATUS));
    }

    private ContainerResponseContext createMockResponseContext() {
        ContainerResponseContext context = Mockito.mock(ContainerResponseContext.class);
        when(context.getHeaders()).thenReturn(new MultivaluedHashMap<>());
        return context;
    }
}