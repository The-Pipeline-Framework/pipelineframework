package org.pipelineframework.context.grpc;

import io.grpc.*;
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

class PipelineContextGrpcClientInterceptorTest {

    private PipelineContextGrpcClientInterceptor interceptor;
    private Channel channel;
    private MethodDescriptor<String, String> method;
    private CallOptions callOptions;

    @BeforeEach
    void setUp() {
        interceptor = new PipelineContextGrpcClientInterceptor();
        channel = mock(Channel.class);
        method = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("test/method")
            .setRequestMarshaller(mock(MethodDescriptor.Marshaller.class))
            .setResponseMarshaller(mock(MethodDescriptor.Marshaller.class))
            .build();
        callOptions = CallOptions.DEFAULT;
        PipelineContextHolder.clear();
        PipelineCacheStatusHolder.clear();
    }

    @Test
    void propagatesContextHeaders() {
        PipelineContext context = new PipelineContext("v1", "true", "prefer-cache");
        PipelineContextHolder.set(context);

        ClientCall<String, String> mockCall = mock(ClientCall.class);
        doReturn(mockCall).when(channel).newCall(any(), any());

        ClientCall<String, String> interceptedCall = interceptor.interceptCall(method, callOptions, channel);

        ClientCall.Listener<String> listener = mock(ClientCall.Listener.class);
        Metadata headers = new Metadata();

        interceptedCall.start(listener, headers);

        assertEquals("v1", headers.get(Metadata.Key.of(PipelineContextHeaders.VERSION, Metadata.ASCII_STRING_MARSHALLER)));
        assertEquals("true", headers.get(Metadata.Key.of(PipelineContextHeaders.REPLAY, Metadata.ASCII_STRING_MARSHALLER)));
        assertEquals("prefer-cache", headers.get(Metadata.Key.of(PipelineContextHeaders.CACHE_POLICY, Metadata.ASCII_STRING_MARSHALLER)));

        PipelineContextHolder.clear();
    }

    @Test
    void skipsNullContextValues() {
        PipelineContext context = new PipelineContext("v2", null, "");
        PipelineContextHolder.set(context);

        ClientCall<String, String> mockCall = mock(ClientCall.class);
        doReturn(mockCall).when(channel).newCall(any(), any());

        ClientCall<String, String> interceptedCall = interceptor.interceptCall(method, callOptions, channel);

        ClientCall.Listener<String> listener = mock(ClientCall.Listener.class);
        Metadata headers = new Metadata();

        interceptedCall.start(listener, headers);

        assertEquals("v2", headers.get(Metadata.Key.of(PipelineContextHeaders.VERSION, Metadata.ASCII_STRING_MARSHALLER)));
        assertNull(headers.get(Metadata.Key.of(PipelineContextHeaders.REPLAY, Metadata.ASCII_STRING_MARSHALLER)));
        assertNull(headers.get(Metadata.Key.of(PipelineContextHeaders.CACHE_POLICY, Metadata.ASCII_STRING_MARSHALLER)));

        PipelineContextHolder.clear();
    }

    @Test
    void extractsCacheStatusFromResponse() {
        ClientCall<String, String> mockCall = mock(ClientCall.class);
        doReturn(mockCall).when(channel).newCall(any(), any());

        ClientCall<String, String> interceptedCall = interceptor.interceptCall(method, callOptions, channel);

        ClientCall.Listener<String> listener = mock(ClientCall.Listener.class);
        Metadata requestHeaders = new Metadata();

        // Capture the wrapped listener
        doAnswer(invocation -> {
            ClientCall.Listener<String> wrappedListener = invocation.getArgument(0);

            // Simulate receiving headers with cache status
            Metadata responseHeaders = new Metadata();
            responseHeaders.put(
                Metadata.Key.of(PipelineContextHeaders.CACHE_STATUS, Metadata.ASCII_STRING_MARSHALLER),
                "HIT");

            wrappedListener.onHeaders(responseHeaders);

            return null;
        }).when(mockCall).start(any(), any());

        interceptedCall.start(listener, requestHeaders);

        assertEquals(CacheStatus.HIT, PipelineCacheStatusHolder.get());
        PipelineCacheStatusHolder.clear();
    }

    @Test
    void handlesNullContext() {
        PipelineContextHolder.clear();

        ClientCall<String, String> mockCall = mock(ClientCall.class);
        doReturn(mockCall).when(channel).newCall(any(), any());

        ClientCall<String, String> interceptedCall = interceptor.interceptCall(method, callOptions, channel);

        ClientCall.Listener<String> listener = mock(ClientCall.Listener.class);
        Metadata headers = new Metadata();

        interceptedCall.start(listener, headers);

        assertNull(headers.get(Metadata.Key.of(PipelineContextHeaders.VERSION, Metadata.ASCII_STRING_MARSHALLER)));
        assertNull(headers.get(Metadata.Key.of(PipelineContextHeaders.REPLAY, Metadata.ASCII_STRING_MARSHALLER)));
        assertNull(headers.get(Metadata.Key.of(PipelineContextHeaders.CACHE_POLICY, Metadata.ASCII_STRING_MARSHALLER)));
    }
}
