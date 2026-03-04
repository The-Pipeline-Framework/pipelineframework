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

import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class PipelineContextClientRequestFilterTest {

    @AfterEach
    void cleanup() {
        PipelineContextHolder.clear();
        TransportDispatchMetadataHolder.clear();
    }

    @Test
    void propagatesPipelineContextHeaders() {
        PipelineContextClientRequestFilter filter = new PipelineContextClientRequestFilter();
        ClientRequestContext context = createMockContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(context.getHeaders()).thenReturn(headers);

        PipelineContextHolder.set(new PipelineContext("v1", "true", "prefer-cache"));

        filter.filter(context);

        assertEquals("v1", headers.getFirst(PipelineContextHeaders.VERSION));
        assertEquals("true", headers.getFirst(PipelineContextHeaders.REPLAY));
        assertEquals("prefer-cache", headers.getFirst(PipelineContextHeaders.CACHE_POLICY));
    }

    @Test
    void propagatesTransportDispatchMetadataHeaders() {
        PipelineContextClientRequestFilter filter = new PipelineContextClientRequestFilter();
        ClientRequestContext context = createMockContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(context.getHeaders()).thenReturn(headers);

        PipelineContextHolder.set(new PipelineContext("v1", "true", "prefer-cache"));
        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-123", "exec-456", "idem-789", 2, 2000000000000L, 1999999999999L, "parent-abc"));

        filter.filter(context);

        assertEquals("corr-123", headers.getFirst(PipelineContextHeaders.TPF_CORRELATION_ID));
        assertEquals("exec-456", headers.getFirst(PipelineContextHeaders.TPF_EXECUTION_ID));
        assertEquals("idem-789", headers.getFirst(PipelineContextHeaders.TPF_IDEMPOTENCY_KEY));
        assertEquals("2", headers.getFirst(PipelineContextHeaders.TPF_RETRY_ATTEMPT));
        assertEquals("2000000000000", headers.getFirst(PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS));
        assertEquals("1999999999999", headers.getFirst(PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS));
        assertEquals("parent-abc", headers.getFirst(PipelineContextHeaders.TPF_PARENT_ITEM_ID));
    }

    @Test
    void doesNotAddHeadersWhenContextIsNull() {
        PipelineContextClientRequestFilter filter = new PipelineContextClientRequestFilter();
        ClientRequestContext context = createMockContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(context.getHeaders()).thenReturn(headers);

        filter.filter(context);

        assertNull(headers.getFirst(PipelineContextHeaders.VERSION));
        assertNull(headers.getFirst(PipelineContextHeaders.REPLAY));
        assertNull(headers.getFirst(PipelineContextHeaders.CACHE_POLICY));
    }

    @Test
    void doesNotOverwriteExistingHeaders() {
        PipelineContextClientRequestFilter filter = new PipelineContextClientRequestFilter();
        ClientRequestContext context = createMockContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.putSingle(PipelineContextHeaders.VERSION, "existing-version");
        when(context.getHeaders()).thenReturn(headers);

        PipelineContextHolder.set(new PipelineContext("new-version", "true", "prefer-cache"));

        filter.filter(context);

        assertEquals("existing-version", headers.getFirst(PipelineContextHeaders.VERSION));
    }

    @Test
    void ignoresBlankValuesInContext() {
        PipelineContextClientRequestFilter filter = new PipelineContextClientRequestFilter();
        ClientRequestContext context = createMockContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(context.getHeaders()).thenReturn(headers);

        PipelineContextHolder.set(new PipelineContext("  ", "", null));

        filter.filter(context);

        assertNull(headers.getFirst(PipelineContextHeaders.VERSION));
        assertNull(headers.getFirst(PipelineContextHeaders.REPLAY));
        assertNull(headers.getFirst(PipelineContextHeaders.CACHE_POLICY));
    }

    @Test
    void propagatesPartialTransportMetadata() {
        PipelineContextClientRequestFilter filter = new PipelineContextClientRequestFilter();
        ClientRequestContext context = createMockContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(context.getHeaders()).thenReturn(headers);

        PipelineContextHolder.set(new PipelineContext("v1", "no", "bypass-cache"));
        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-only", null, null, null, null, null, null));

        filter.filter(context);

        assertEquals("corr-only", headers.getFirst(PipelineContextHeaders.TPF_CORRELATION_ID));
        assertNull(headers.getFirst(PipelineContextHeaders.TPF_EXECUTION_ID));
        assertNull(headers.getFirst(PipelineContextHeaders.TPF_IDEMPOTENCY_KEY));
    }

    @Test
    void convertsNumericFieldsToStrings() {
        PipelineContextClientRequestFilter filter = new PipelineContextClientRequestFilter();
        ClientRequestContext context = createMockContext();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        when(context.getHeaders()).thenReturn(headers);

        PipelineContextHolder.set(new PipelineContext("v1", "no", "bypass-cache"));
        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            null, null, null, 0, 0L, 0L, null));

        filter.filter(context);

        assertEquals("0", headers.getFirst(PipelineContextHeaders.TPF_RETRY_ATTEMPT));
        assertEquals("0", headers.getFirst(PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS));
        assertEquals("0", headers.getFirst(PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS));
    }

    private ClientRequestContext createMockContext() {
        return Mockito.mock(ClientRequestContext.class);
    }
}