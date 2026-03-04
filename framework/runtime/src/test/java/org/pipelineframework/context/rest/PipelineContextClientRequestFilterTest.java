package org.pipelineframework.context.rest;

import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineContextClientRequestFilterTest {

    private PipelineContextClientRequestFilter filter;
    private ClientRequestContext requestContext;

    @BeforeEach
    void setUp() {
        filter = new PipelineContextClientRequestFilter();
        requestContext = mock(ClientRequestContext.class);
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(requestContext.getHeaders()).thenReturn(headers);
        PipelineContextHolder.clear();
    }

    @AfterEach
    void tearDown() {
        PipelineContextHolder.clear();
    }

    @Test
    void addsContextHeadersToRequest() {
        PipelineContext context = new PipelineContext("v1", "true", "prefer-cache");
        PipelineContextHolder.set(context);

        filter.filter(requestContext);

        MultivaluedMap<String, Object> headers = requestContext.getHeaders();
        assertEquals("v1", headers.getFirst(PipelineContextHeaders.VERSION));
        assertEquals("true", headers.getFirst(PipelineContextHeaders.REPLAY));
        assertEquals("prefer-cache", headers.getFirst(PipelineContextHeaders.CACHE_POLICY));
    }

    @Test
    void skipsNullContext() {
        PipelineContextHolder.clear();

        filter.filter(requestContext);

        MultivaluedMap<String, Object> headers = requestContext.getHeaders();
        assertNull(headers.getFirst(PipelineContextHeaders.VERSION));
        assertNull(headers.getFirst(PipelineContextHeaders.REPLAY));
        assertNull(headers.getFirst(PipelineContextHeaders.CACHE_POLICY));
    }

    @Test
    void skipsBlankValues() {
        PipelineContext context = new PipelineContext("v2", "", null);
        PipelineContextHolder.set(context);

        filter.filter(requestContext);

        MultivaluedMap<String, Object> headers = requestContext.getHeaders();
        assertEquals("v2", headers.getFirst(PipelineContextHeaders.VERSION));
        assertNull(headers.getFirst(PipelineContextHeaders.REPLAY));
        assertNull(headers.getFirst(PipelineContextHeaders.CACHE_POLICY));
    }

    @Test
    void handlesWhitespaceValues() {
        PipelineContext context = new PipelineContext("v3", "   ", "bypass-cache");
        PipelineContextHolder.set(context);

        filter.filter(requestContext);

        MultivaluedMap<String, Object> headers = requestContext.getHeaders();
        assertEquals("v3", headers.getFirst(PipelineContextHeaders.VERSION));
        assertNull(headers.getFirst(PipelineContextHeaders.REPLAY));
        assertEquals("bypass-cache", headers.getFirst(PipelineContextHeaders.CACHE_POLICY));
    }
}