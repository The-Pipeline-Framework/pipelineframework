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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

class PipelineContextRequestFilterTest {

    @AfterEach
    void cleanup() {
        PipelineContextHolder.clear();
        TransportDispatchMetadataHolder.clear();
    }

    @Test
    void extractsPipelineContextFromHeaders() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        ContainerRequestContext context = createMockContext(
            "v1", "true", "prefer-cache",
            null, null, null, null, null, null, null);

        filter.filter(context);

        PipelineContext pipelineContext = PipelineContextHolder.get();
        assertNotNull(pipelineContext);
        assertEquals("v1", pipelineContext.versionTag());
        assertEquals("true", pipelineContext.replayMode());
        assertEquals("prefer-cache", pipelineContext.cachePolicy());
    }

    @Test
    void extractsTransportDispatchMetadataFromHeaders() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        ContainerRequestContext context = createMockContext(
            null, null, null,
            "corr-123", "exec-456", "idem-789", "2", "2000000000000", "1999999999999", "parent-abc");

        filter.filter(context);

        TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
        assertNotNull(metadata);
        assertEquals("corr-123", metadata.correlationId());
        assertEquals("exec-456", metadata.executionId());
        assertEquals("idem-789", metadata.idempotencyKey());
        assertEquals(2, metadata.retryAttempt());
        assertEquals(2000000000000L, metadata.deadlineEpochMs());
        assertEquals(1999999999999L, metadata.dispatchTsEpochMs());
        assertEquals("parent-abc", metadata.parentItemId());
    }

    @Test
    void handlesNullHeaders() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        ContainerRequestContext context = createMockContext(
            null, null, null,
            null, null, null, null, null, null, null);

        filter.filter(context);

        PipelineContext pipelineContext = PipelineContextHolder.get();
        assertNotNull(pipelineContext);
        assertNull(pipelineContext.versionTag());
        assertNull(pipelineContext.replayMode());
        assertNull(pipelineContext.cachePolicy());

        TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
        assertNotNull(metadata);
        assertNull(metadata.correlationId());
        assertNull(metadata.executionId());
    }

    @Test
    void handlesPartialHeaders() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        ContainerRequestContext context = createMockContext(
            "v2", null, "bypass-cache",
            "corr-only", null, null, null, null, null, null);

        filter.filter(context);

        PipelineContext pipelineContext = PipelineContextHolder.get();
        assertEquals("v2", pipelineContext.versionTag());
        assertNull(pipelineContext.replayMode());
        assertEquals("bypass-cache", pipelineContext.cachePolicy());

        TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
        assertEquals("corr-only", metadata.correlationId());
        assertNull(metadata.executionId());
    }

    @Test
    void parsesNumericHeaders() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        long now = System.currentTimeMillis();
        ContainerRequestContext context = createMockContext(
            null, null, null,
            null, null, null, "0", Long.toString(now + 60_000L), Long.toString(now), null);

        filter.filter(context);

        TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
        assertEquals(0, metadata.retryAttempt());
        assertEquals(now + 60_000L, metadata.deadlineEpochMs());
        assertEquals(now, metadata.dispatchTsEpochMs());
    }

    @Test
    void handlesInvalidNumericHeaders() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        ContainerRequestContext context = createMockContext(
            null, null, null,
            "corr", "exec", "idem", "not-a-number", "invalid-long", "also-invalid", null);

        filter.filter(context);

        TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
        assertEquals("corr", metadata.correlationId());
        assertNull(metadata.retryAttempt());
        assertNull(metadata.deadlineEpochMs());
        assertNull(metadata.dispatchTsEpochMs());
    }

    @Test
    void storesBothPipelineContextAndTransportMetadata() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        long now = System.currentTimeMillis();
        ContainerRequestContext context = createMockContext(
            "v3", "false", "require-cache",
            "corr-999", "exec-888", "idem-777", "5",
            Long.toString(now + 300_000L),
            Long.toString(now),
            "parent-xyz");

        filter.filter(context);

        PipelineContext pipelineContext = PipelineContextHolder.get();
        assertNotNull(pipelineContext);
        assertEquals("v3", pipelineContext.versionTag());

        TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
        assertNotNull(metadata);
        assertEquals("corr-999", metadata.correlationId());
        assertEquals(5, metadata.retryAttempt());
    }

    @Test
    void rejectsExpiredDeadlineBeforeExecution() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        ContainerRequestContext context = createMockContext(
            null, null, null,
            "corr-1", "exec-1", "idem-1", "0",
            Long.toString(System.currentTimeMillis() - 1_000L),
            "1699999999999", null);

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> filter.filter(context));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, ex.getStatus().getCode());
        assertNull(PipelineContextHolder.get());
        assertNull(TransportDispatchMetadataHolder.get());
    }

    private ContainerRequestContext createMockContext(
        String version, String replay, String cachePolicy,
        String correlationId, String executionId, String idempotencyKey,
        String retryAttempt, String deadlineEpochMs, String dispatchTsEpochMs, String parentItemId) {
        ContainerRequestContext context = Mockito.mock(ContainerRequestContext.class);
        when(context.getHeaderString(PipelineContextHeaders.VERSION)).thenReturn(version);
        when(context.getHeaderString(PipelineContextHeaders.REPLAY)).thenReturn(replay);
        when(context.getHeaderString(PipelineContextHeaders.CACHE_POLICY)).thenReturn(cachePolicy);
        when(context.getHeaderString(PipelineContextHeaders.TPF_CORRELATION_ID)).thenReturn(correlationId);
        when(context.getHeaderString(PipelineContextHeaders.TPF_EXECUTION_ID)).thenReturn(executionId);
        when(context.getHeaderString(PipelineContextHeaders.TPF_IDEMPOTENCY_KEY)).thenReturn(idempotencyKey);
        when(context.getHeaderString(PipelineContextHeaders.TPF_RETRY_ATTEMPT)).thenReturn(retryAttempt);
        when(context.getHeaderString(PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS)).thenReturn(deadlineEpochMs);
        when(context.getHeaderString(PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS)).thenReturn(dispatchTsEpochMs);
        when(context.getHeaderString(PipelineContextHeaders.TPF_PARENT_ITEM_ID)).thenReturn(parentItemId);
        return context;
    }
}
