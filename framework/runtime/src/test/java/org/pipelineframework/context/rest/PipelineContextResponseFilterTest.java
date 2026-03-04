package org.pipelineframework.context.rest;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineContextResponseFilterTest {

    private PipelineContextResponseFilter filter;
    private ContainerRequestContext requestContext;
    private ContainerResponseContext responseContext;

    @BeforeEach
    void setUp() {
        filter = new PipelineContextResponseFilter();
        requestContext = mock(ContainerRequestContext.class);
        responseContext = mock(ContainerResponseContext.class);
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(responseContext.getHeaders()).thenReturn(headers);
        PipelineContextHolder.clear();
        PipelineCacheStatusHolder.clear();
    }

    @AfterEach
    void tearDown() {
        PipelineContextHolder.clear();
        PipelineCacheStatusHolder.clear();
    }

    @Test
    void addsCacheStatusHeaderWhenPresent() {
        PipelineCacheStatusHolder.set(CacheStatus.HIT);

        filter.filter(requestContext, responseContext);

        MultivaluedMap<String, Object> headers = responseContext.getHeaders();
        assertEquals("HIT", headers.getFirst(PipelineContextHeaders.CACHE_STATUS));
        assertNull(PipelineCacheStatusHolder.get());
    }

    @Test
    void clearsContextAfterResponse() {
        PipelineContext context = new PipelineContext("v1", "true", "prefer-cache");
        PipelineContextHolder.set(context);

        filter.filter(requestContext, responseContext);

        assertNull(PipelineContextHolder.get());
    }

    @Test
    void handlesNullCacheStatus() {
        PipelineCacheStatusHolder.clear();

        filter.filter(requestContext, responseContext);

        MultivaluedMap<String, Object> headers = responseContext.getHeaders();
        assertNull(headers.getFirst(PipelineContextHeaders.CACHE_STATUS));
    }

    @Test
    void clearsCacheStatusAfterAddingToResponse() {
        PipelineCacheStatusHolder.set(CacheStatus.MISS);

        filter.filter(requestContext, responseContext);

        assertNull(PipelineCacheStatusHolder.get());
    }

    @Test
    void handlesMultipleCacheStatuses() {
        PipelineCacheStatusHolder.set(CacheStatus.BYPASS);

        filter.filter(requestContext, responseContext);

        MultivaluedMap<String, Object> headers = responseContext.getHeaders();
        assertEquals("BYPASS", headers.getFirst(PipelineContextHeaders.CACHE_STATUS));
    }
}
