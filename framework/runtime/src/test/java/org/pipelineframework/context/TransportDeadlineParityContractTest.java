/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.context;

import java.util.Map;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.container.ContainerRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.context.grpc.PipelineContextGrpcServerInterceptor;
import org.pipelineframework.context.rest.PipelineContextRequestFilter;
import org.pipelineframework.transport.function.DefaultUnaryFunctionSinkAdapter;
import org.pipelineframework.transport.function.DefaultUnaryFunctionSourceAdapter;
import org.pipelineframework.transport.function.FunctionInvocationMode;
import org.pipelineframework.transport.function.FunctionTransportBridge;
import org.pipelineframework.transport.function.FunctionTransportContext;
import org.pipelineframework.transport.function.InvocationModeRoutingFunctionInvokeAdapter;
import org.pipelineframework.transport.function.LocalUnaryFunctionInvokeAdapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TransportDeadlineParityContractTest {

    @AfterEach
    void cleanupHolders() {
        PipelineContextHolder.clear();
        TransportDispatchMetadataHolder.clear();
    }

    @ParameterizedTest
    @EnumSource(TransportPath.class)
    void expiredDeadlineFailsWithDeadlineExceededAcrossTransports(TransportPath path) {
        assertEquals(Status.Code.DEADLINE_EXCEEDED, invokeWithExpiredDeadline(path));
    }

    private Status.Code invokeWithExpiredDeadline(TransportPath path) {
        return switch (path) {
            case REST -> invokeRest();
            case GRPC -> invokeGrpc();
            case FUNCTION -> invokeFunctionRemote();
            case LOCAL -> invokeFunctionLocal();
        };
    }

    private Status.Code invokeRest() {
        PipelineContextRequestFilter filter = new PipelineContextRequestFilter();
        ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
        when(requestContext.getHeaderString(PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS))
            .thenReturn(Long.toString(System.currentTimeMillis() - 1_000L));
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> filter.filter(requestContext));
        return ex.getStatus().getCode();
    }

    @SuppressWarnings("unchecked")
    private Status.Code invokeGrpc() {
        PipelineContextGrpcServerInterceptor interceptor = new PipelineContextGrpcServerInterceptor();
        ServerCall<String, String> serverCall = mock(ServerCall.class);
        ServerCallHandler<String, String> handler = mock(ServerCallHandler.class);
        when(handler.startCall(any(), any())).thenReturn(mock(ServerCall.Listener.class));

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of(PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS, Metadata.ASCII_STRING_MARSHALLER),
            Long.toString(System.currentTimeMillis() - 1_000L));

        interceptor.interceptCall(serverCall, headers, handler);

        ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
        verify(serverCall).close(statusCaptor.capture(), any(Metadata.class));
        verify(handler, never()).startCall(any(), any());
        return statusCaptor.getValue().getCode();
    }

    private Status.Code invokeFunctionLocal() {
        FunctionTransportContext context = FunctionTransportContext.of(
            "req-local-deadline",
            "search-handler",
            "invoke-step",
            Map.of(FunctionTransportContext.ATTR_DEADLINE_EPOCH_MS,
                Long.toString(System.currentTimeMillis() - 1_000L)));

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
            () -> FunctionTransportBridge.invokeOneToOne(
                "hello",
                context,
                new DefaultUnaryFunctionSourceAdapter<>("search.in", "v1"),
                new LocalUnaryFunctionInvokeAdapter<>(payload -> Uni.createFrom().item(payload.length()), "search.out", "v1"),
                new DefaultUnaryFunctionSinkAdapter<>()));
        return ex.getStatus().getCode();
    }

    private Status.Code invokeFunctionRemote() {
        FunctionTransportContext context = FunctionTransportContext.of(
            "req-remote-deadline",
            "search-handler",
            "invoke-step",
            Map.of(
                FunctionTransportContext.ATTR_DEADLINE_EPOCH_MS, Long.toString(System.currentTimeMillis() - 1_000L),
                FunctionTransportContext.ATTR_INVOCATION_MODE, FunctionInvocationMode.REMOTE.name(),
                FunctionTransportContext.ATTR_TARGET_RUNTIME, "pipeline",
                FunctionTransportContext.ATTR_TARGET_MODULE, "mod",
                FunctionTransportContext.ATTR_TARGET_HANDLER, "handler"));

        InvocationModeRoutingFunctionInvokeAdapter<String, Integer> invokeAdapter =
            new InvocationModeRoutingFunctionInvokeAdapter<>(
                new LocalUnaryFunctionInvokeAdapter<>(
                    payload -> Uni.createFrom().item(payload.length()),
                    "search.out",
                    "v1"),
                new LocalUnaryFunctionInvokeAdapter<>(
                    payload -> Uni.createFrom().item(payload.length()),
                    "search.out",
                    "v1"));

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
            () -> FunctionTransportBridge.invokeOneToOne(
                "hello",
                context,
                new DefaultUnaryFunctionSourceAdapter<>("search.in", "v1"),
                invokeAdapter,
                new DefaultUnaryFunctionSinkAdapter<>()));
        return ex.getStatus().getCode();
    }

    private enum TransportPath {
        REST,
        GRPC,
        FUNCTION,
        LOCAL
    }
}
