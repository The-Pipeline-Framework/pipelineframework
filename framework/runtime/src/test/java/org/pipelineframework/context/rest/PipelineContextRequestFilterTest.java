package org.pipelineframework.context.rest;

import jakarta.ws.rs.container.ContainerRequestContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineContextRequestFilterTest {

    private PipelineContextRequestFilter filter;
    private ContainerRequestContext requestContext;

    @BeforeEach
    void setUp() {
        filter = new PipelineContextRequestFilter();
        requestContext = mock(ContainerRequestContext.class);
        PipelineContextHolder.clear();
    }

    @AfterEach
    void tearDown() {
        PipelineContextHolder.clear();
    }

    @Test
    void extractsContextFromRequestHeaders() {
        when(requestContext.getHeaderString(PipelineContextHeaders.VERSION)).thenReturn("v1");
        when(requestContext.getHeaderString(PipelineContextHeaders.REPLAY)).thenReturn("true");
        when(requestContext.getHeaderString(PipelineContextHeaders.CACHE_POLICY)).thenReturn("prefer-cache");

        filter.filter(requestContext);

        PipelineContext context = PipelineContextHolder.get();
        assertNotNull(context);
        assertEquals("v1", context.versionTag());
        assertEquals("true", context.replayMode());
        assertEquals("prefer-cache", context.cachePolicy());
    }

    @Test
    void handlesNullHeaders() {
        when(requestContext.getHeaderString(any())).thenReturn(null);

        filter.filter(requestContext);

        PipelineContext context = PipelineContextHolder.get();
        assertNotNull(context);
        assertNull(context.versionTag());
        assertNull(context.replayMode());
        assertNull(context.cachePolicy());
    }

    @Test
    void handlesPartialHeaders() {
        when(requestContext.getHeaderString(PipelineContextHeaders.VERSION)).thenReturn("v2");
        when(requestContext.getHeaderString(PipelineContextHeaders.REPLAY)).thenReturn(null);
        when(requestContext.getHeaderString(PipelineContextHeaders.CACHE_POLICY)).thenReturn("bypass-cache");

        filter.filter(requestContext);

        PipelineContext context = PipelineContextHolder.get();
        assertNotNull(context);
        assertEquals("v2", context.versionTag());
        assertNull(context.replayMode());
        assertEquals("bypass-cache", context.cachePolicy());
    }
}