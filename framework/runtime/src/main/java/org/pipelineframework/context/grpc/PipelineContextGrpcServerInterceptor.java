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

package org.pipelineframework.context.grpc;

import jakarta.enterprise.context.ApplicationScoped;

import io.grpc.*;
import io.quarkus.arc.Unremovable;
import io.quarkus.grpc.GlobalInterceptor;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

/**
 * Extracts pipeline context headers on gRPC server calls.
 */
@ApplicationScoped
@Unremovable
@GlobalInterceptor
public class PipelineContextGrpcServerInterceptor implements ServerInterceptor {

    /**
     * Creates a new PipelineContextGrpcServerInterceptor.
     */
    public PipelineContextGrpcServerInterceptor() {
    }

    private static final Metadata.Key<String> VERSION_HEADER =
        Metadata.Key.of(PipelineContextHeaders.VERSION, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> REPLAY_HEADER =
        Metadata.Key.of(PipelineContextHeaders.REPLAY, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> CACHE_POLICY_HEADER =
        Metadata.Key.of(PipelineContextHeaders.CACHE_POLICY, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> CACHE_STATUS_HEADER =
        Metadata.Key.of(PipelineContextHeaders.CACHE_STATUS, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> CORRELATION_ID_HEADER =
        Metadata.Key.of(PipelineContextHeaders.TPF_CORRELATION_ID, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> EXECUTION_ID_HEADER =
        Metadata.Key.of(PipelineContextHeaders.TPF_EXECUTION_ID, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> IDEMPOTENCY_KEY_HEADER =
        Metadata.Key.of(PipelineContextHeaders.TPF_IDEMPOTENCY_KEY, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> RETRY_ATTEMPT_HEADER =
        Metadata.Key.of(PipelineContextHeaders.TPF_RETRY_ATTEMPT, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DEADLINE_EPOCH_MS_HEADER =
        Metadata.Key.of(PipelineContextHeaders.TPF_DEADLINE_EPOCH_MS, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DISPATCH_TS_EPOCH_MS_HEADER =
        Metadata.Key.of(PipelineContextHeaders.TPF_DISPATCH_TS_EPOCH_MS, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PARENT_ITEM_ID_HEADER =
        Metadata.Key.of(PipelineContextHeaders.TPF_PARENT_ITEM_ID, Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Intercepts a gRPC server call to extract request-side pipeline and dispatch metadata, make them available
     * for the duration of the call, and ensure cache status is propagated to response headers.
     *
     * Extracts PipelineContext (version, replay, cache policy) and TransportDispatchMetadata
     * (correlation id, execution id, idempotency key, retry attempt, deadline epoch ms, dispatch timestamp epoch ms,
     * parent item id) from the incoming request headers and stores them in their respective holders.
     * When response headers are sent, writes the current cache status (if any) under the configured cache status header.
     * Clears both holders when the call completes or is cancelled.
     *
     * @param call the incoming server call being intercepted
     * @param headers the request metadata containing pipeline and dispatch headers
     * @param next the next handler in the interceptor chain
     * @return a ServerCall.Listener that delegates to the next handler while ensuring header injection and holder cleanup
     */
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        Metadata headers,
        ServerCallHandler<ReqT, RespT> next) {

        PipelineContext context = PipelineContext.fromHeaders(
            headers.get(VERSION_HEADER),
            headers.get(REPLAY_HEADER),
            headers.get(CACHE_POLICY_HEADER));
        PipelineContextHolder.set(context);
        TransportDispatchMetadata transportDispatchMetadata = TransportDispatchMetadata.fromHeaders(
            headers.get(CORRELATION_ID_HEADER),
            headers.get(EXECUTION_ID_HEADER),
            headers.get(IDEMPOTENCY_KEY_HEADER),
            headers.get(RETRY_ATTEMPT_HEADER),
            headers.get(DEADLINE_EPOCH_MS_HEADER),
            headers.get(DISPATCH_TS_EPOCH_MS_HEADER),
            headers.get(PARENT_ITEM_ID_HEADER));
        TransportDispatchMetadataHolder.set(transportDispatchMetadata);

        ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void sendHeaders(Metadata responseHeaders) {
                CacheStatus status = PipelineCacheStatusHolder.getAndClear();
                if (status != null) {
                    responseHeaders.put(CACHE_STATUS_HEADER, status.name());
                }
                super.sendHeaders(responseHeaders);
            }
        };

        ServerCall.Listener<ReqT> listener = next.startCall(wrappedCall, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onComplete() {
                PipelineContextHolder.clear();
                TransportDispatchMetadataHolder.clear();
                super.onComplete();
            }

            @Override
            public void onCancel() {
                PipelineContextHolder.clear();
                TransportDispatchMetadataHolder.clear();
                super.onCancel();
            }
        };
    }
}
