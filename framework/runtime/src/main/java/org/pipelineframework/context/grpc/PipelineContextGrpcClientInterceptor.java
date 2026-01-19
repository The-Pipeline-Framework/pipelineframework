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
 * Propagates pipeline context headers on gRPC client calls.
 */
@ApplicationScoped
@Unremovable
@GlobalInterceptor
public class PipelineContextGrpcClientInterceptor implements ClientInterceptor {

    /**
     * Creates a new PipelineContextGrpcClientInterceptor.
     */
    public PipelineContextGrpcClientInterceptor() {
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
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method,
        CallOptions callOptions,
        Channel next) {

        PipelineContext context = PipelineContextHolder.get();
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                Listener<RespT> wrapped = new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                    @Override
                    public void onHeaders(Metadata headers) {
                        CacheStatus status = CacheStatus.fromHeader(headers.get(CACHE_STATUS_HEADER));
                        PipelineCacheStatusHolder.set(status);
                        super.onHeaders(headers);
                    }
                };
                if (context != null) {
                    putIfPresent(headers, VERSION_HEADER, context.versionTag());
                    putIfPresent(headers, REPLAY_HEADER, context.replayMode());
                    putIfPresent(headers, CACHE_POLICY_HEADER, context.cachePolicy());
                }
                super.start(wrapped, headers);
            }
        };
    }

    private static void putIfPresent(Metadata headers, Metadata.Key<String> key, String value) {
        if (value != null && !value.isBlank()) {
            headers.put(key, value);
        }
    }
}
