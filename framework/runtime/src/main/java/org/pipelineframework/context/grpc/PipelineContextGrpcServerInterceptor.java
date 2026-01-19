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
                super.onComplete();
            }

            @Override
            public void onCancel() {
                PipelineContextHolder.clear();
                super.onCancel();
            }
        };
    }
}
