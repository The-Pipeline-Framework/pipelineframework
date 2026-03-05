package org.pipelineframework.context.grpc;

import java.util.concurrent.atomic.AtomicReference;

import io.grpc.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PipelineContextGrpcServerInterceptorTest {

    private PipelineContextGrpcServerInterceptor interceptor;
    private ServerCall<String, String> serverCall;
    private ServerCallHandler<String, String> handler;

    @BeforeEach
    void setUp() {
        interceptor = new PipelineContextGrpcServerInterceptor();
        serverCall = mock(ServerCall.class);
        handler = mock(ServerCallHandler.class);
        when(handler.startCall(any(), any())).thenReturn(mock(ServerCall.Listener.class));
        PipelineContextHolder.clear();
        PipelineCacheStatusHolder.clear();
    }

    @AfterEach
    void tearDown() {
        PipelineContextHolder.clear();
        PipelineCacheStatusHolder.clear();
    }

    @Test
    void extractsContextFromHeaders() {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of(PipelineContextHeaders.VERSION, Metadata.ASCII_STRING_MARSHALLER), "v3");
        headers.put(Metadata.Key.of(PipelineContextHeaders.REPLAY, Metadata.ASCII_STRING_MARSHALLER), "yes");
        headers.put(Metadata.Key.of(PipelineContextHeaders.CACHE_POLICY, Metadata.ASCII_STRING_MARSHALLER), "bypass-cache");

        interceptor.interceptCall(serverCall, headers, handler);

        PipelineContext context = PipelineContextHolder.get();
        assertNotNull(context);
        assertEquals("v3", context.versionTag());
        assertEquals("yes", context.replayMode());
        assertEquals("bypass-cache", context.cachePolicy());
    }

    @Test
    void handlesNullHeaders() {
        Metadata headers = new Metadata();

        interceptor.interceptCall(serverCall, headers, handler);

        PipelineContext context = PipelineContextHolder.get();
        assertNotNull(context);
        assertNull(context.versionTag());
        assertNull(context.replayMode());
        assertNull(context.cachePolicy());
    }

    @Test
    void propagatesCacheStatusToResponse() {
        Metadata headers = new Metadata();
        PipelineCacheStatusHolder.set(CacheStatus.MISS);
        AtomicReference<ServerCall<String, String>> wrappedCallRef = new AtomicReference<>();

        Metadata[] capturedHeaders = new Metadata[1];
        doAnswer(invocation -> {
            capturedHeaders[0] = invocation.getArgument(0);
            return null;
        }).when(serverCall).sendHeaders(any(Metadata.class));
        when(handler.startCall(any(), any())).thenAnswer(invocation -> {
            wrappedCallRef.set(invocation.getArgument(0));
            return mock(ServerCall.Listener.class);
        });

        interceptor.interceptCall(serverCall, headers, handler);
        ServerCall<String, String> wrappedCall = wrappedCallRef.get();
        assertNotNull(wrappedCall);

        // Simulate sending headers
        wrappedCall.sendHeaders(new Metadata());

        assertNotNull(capturedHeaders[0]);
        String status = capturedHeaders[0].get(
            Metadata.Key.of(PipelineContextHeaders.CACHE_STATUS, Metadata.ASCII_STRING_MARSHALLER));
        assertEquals("MISS", status);
    }

    @Test
    void clearContextOnComplete() {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of(PipelineContextHeaders.VERSION, Metadata.ASCII_STRING_MARSHALLER), "v4");

        ServerCall.Listener<String> listener = interceptor.interceptCall(serverCall, headers, handler);

        assertNotNull(PipelineContextHolder.get());

        listener.onComplete();

        assertNull(PipelineContextHolder.get());
    }

    @Test
    void clearsContextOnCancel() {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of(PipelineContextHeaders.VERSION, Metadata.ASCII_STRING_MARSHALLER), "v5");

        ServerCall.Listener<String> listener = interceptor.interceptCall(serverCall, headers, handler);

        assertNotNull(PipelineContextHolder.get());

        listener.onCancel();

        assertNull(PipelineContextHolder.get());
    }
}
