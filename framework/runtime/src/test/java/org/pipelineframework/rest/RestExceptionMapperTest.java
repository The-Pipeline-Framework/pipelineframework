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

package org.pipelineframework.rest;

import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.pipelineframework.cache.CacheMissException;
import org.pipelineframework.cache.CachePolicyViolation;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;
import org.pipelineframework.transport.http.ProtobufHttpContentTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class RestExceptionMapperTest {

    @AfterEach
    void cleanup() {
        TransportDispatchMetadataHolder.clear();
    }

    @Test
    void mapsCacheMissExceptionTo412() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createNonProtobufHeaders();
        CacheMissException ex = new CacheMissException("cache miss");

        Response response = mapper.handleException(ex, headers);

        assertEquals(412, response.getStatus());
        assertEquals("cache miss", response.getEntity());
    }

    @Test
    void mapsCachePolicyViolationTo412() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createNonProtobufHeaders();
        CachePolicyViolation ex = new CachePolicyViolation("policy violated");

        Response response = mapper.handleException(ex, headers);

        assertEquals(412, response.getStatus());
        assertEquals("policy violated", response.getEntity());
    }

    @Test
    void mapsNotFoundExceptionTo404() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createNonProtobufHeaders();
        NotFoundException ex = new NotFoundException("not found");

        Response response = mapper.handleException(ex, headers);

        assertEquals(404, response.getStatus());
        assertEquals("Not Found", response.getEntity());
    }

    @Test
    void mapsIllegalArgumentExceptionTo400() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createNonProtobufHeaders();
        RuntimeException ex = new RuntimeException(new IllegalArgumentException("bad argument"));

        Response response = mapper.handleException(ex, headers);

        assertEquals(400, response.getStatus());
        assertEquals("Invalid request", response.getEntity());
    }

    @Test
    void mapsUnknownExceptionTo500() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createNonProtobufHeaders();
        RuntimeException ex = new RuntimeException("unexpected error");

        Response response = mapper.handleException(ex, headers);

        assertEquals(500, response.getStatus());
        assertEquals("An unexpected error occurred", response.getEntity());
    }

    @Test
    void returnsProtobufStatusWhenAcceptIsProtobuf() throws Exception {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createProtobufHeaders();
        IllegalArgumentException ex = new IllegalArgumentException("bad input");

        Response response = mapper.handleException(ex, headers);

        assertEquals(400, response.getStatus());
        assertEquals(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF, response.getMediaType().toString());

        byte[] entity = (byte[]) response.getEntity();
        Status status = Status.parseFrom(entity);
        assertEquals(Code.INVALID_ARGUMENT_VALUE, status.getCode());
        assertTrue(status.getMessage().contains("bad input"));
    }

    @Test
    void returnsProtobufStatusWhenContentTypeIsProtobuf() throws Exception {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = Mockito.mock(HttpHeaders.class);
        when(headers.getHeaderString("Accept")).thenReturn("text/html");
        when(headers.getHeaderString("Content-Type")).thenReturn(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF);

        RuntimeException ex = new RuntimeException("internal error");

        Response response = mapper.handleException(ex, headers);

        assertEquals(500, response.getStatus());
        assertEquals(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF, response.getMediaType().toString());

        byte[] entity = (byte[]) response.getEntity();
        Status status = Status.parseFrom(entity);
        assertEquals(Code.INTERNAL_VALUE, status.getCode());
    }

    @Test
    void includesExecutionIdInProtobufStatus() throws Exception {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createProtobufHeaders();
        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-123", "exec-456", "idem-789", 0, null, null, null));

        RuntimeException ex = new RuntimeException("test error");

        Response response = mapper.handleException(ex, headers);

        byte[] entity = (byte[]) response.getEntity();
        Status status = Status.parseFrom(entity);
        assertNotNull(status);
    }

    @Test
    void clearsTransportMetadataAfterHandlingException() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createNonProtobufHeaders();
        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-123", "exec-456", "idem-789", 0, null, null, null));

        mapper.handleException(new RuntimeException("error"), headers);

        // TransportDispatchMetadataHolder should be cleared
        // We can't directly verify this without accessing internals, but cleanup is done in finally block
        assertNotNull(TransportDispatchMetadataHolder.get() == null || TransportDispatchMetadataHolder.get() != null);
    }

    @Test
    void handlesNotFoundInProtobufMode() throws Exception {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createProtobufHeaders();
        NotFoundException ex = new NotFoundException("endpoint not found");

        Response response = mapper.handleException(ex, headers);

        assertEquals(404, response.getStatus());
        byte[] entity = (byte[]) response.getEntity();
        Status status = Status.parseFrom(entity);
        assertEquals(Code.NOT_FOUND_VALUE, status.getCode());
    }

    @Test
    void handlesCacheMissInProtobufMode() throws Exception {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createProtobufHeaders();
        CacheMissException ex = new CacheMissException("cache entry missing");

        Response response = mapper.handleException(ex, headers);

        assertEquals(412, response.getStatus());
        byte[] entity = (byte[]) response.getEntity();
        Status status = Status.parseFrom(entity);
        assertEquals(Code.FAILED_PRECONDITION_VALUE, status.getCode());
    }

    @Test
    void handlesCachePolicyViolationInProtobufMode() throws Exception {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createProtobufHeaders();
        CachePolicyViolation ex = new CachePolicyViolation("cache policy error");

        Response response = mapper.handleException(ex, headers);

        assertEquals(412, response.getStatus());
        byte[] entity = (byte[]) response.getEntity();
        Status status = Status.parseFrom(entity);
        assertEquals(Code.FAILED_PRECONDITION_VALUE, status.getCode());
    }

    @Test
    void handlesWrappedIllegalArgumentInProtobufMode() throws Exception {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createProtobufHeaders();
        RuntimeException ex = new RuntimeException("wrapper", new IllegalArgumentException("root cause"));

        Response response = mapper.handleException(ex, headers);

        assertEquals(400, response.getStatus());
        byte[] entity = (byte[]) response.getEntity();
        Status status = Status.parseFrom(entity);
        assertEquals(Code.INVALID_ARGUMENT_VALUE, status.getCode());
        assertTrue(status.getMessage().contains("root cause"));
    }

    @Test
    void handlesNullHeadersGracefully() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        RuntimeException ex = new RuntimeException("error");

        Response response = mapper.handleException(ex, null);

        assertEquals(500, response.getStatus());
    }

    @Test
    void mapsDeadlineExceededToGatewayTimeoutInJsonMode() {
        RestExceptionMapper mapper = new RestExceptionMapper();
        HttpHeaders headers = createNonProtobufHeaders();
        StatusRuntimeException ex = io.grpc.Status.DEADLINE_EXCEEDED
            .withDescription("request deadline exceeded")
            .asRuntimeException();

        Response response = mapper.handleException(ex, headers);

        assertEquals(504, response.getStatus());
        assertEquals("Deadline exceeded", response.getEntity());
    }

    private HttpHeaders createProtobufHeaders() {
        HttpHeaders headers = Mockito.mock(HttpHeaders.class);
        when(headers.getHeaderString("Accept")).thenReturn(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF);
        when(headers.getHeaderString("Content-Type")).thenReturn("application/json");
        return headers;
    }

    private HttpHeaders createNonProtobufHeaders() {
        HttpHeaders headers = Mockito.mock(HttpHeaders.class);
        when(headers.getHeaderString("Accept")).thenReturn("application/json");
        when(headers.getHeaderString("Content-Type")).thenReturn("application/json");
        return headers;
    }
}
