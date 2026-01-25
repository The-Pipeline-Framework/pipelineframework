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

package org.pipelineframework.telemetry;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.grpc.Status;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Adds OpenTelemetry gRPC client spans around reactive calls.
 */
public final class GrpcClientTracing {

    private static final AttributeKey<String> RPC_SYSTEM = AttributeKey.stringKey("rpc.system");
    private static final AttributeKey<String> RPC_SERVICE = AttributeKey.stringKey("rpc.service");
    private static final AttributeKey<String> RPC_METHOD = AttributeKey.stringKey("rpc.method");
    private static final AttributeKey<Long> RPC_GRPC_STATUS = AttributeKey.longKey("rpc.grpc.status_code");
    private static final AttributeKey<String> PEER_SERVICE = AttributeKey.stringKey("peer.service");
    private static final String FORCE_CLIENT_SPANS_KEY = "pipeline.telemetry.tracing.client-spans.force";
    private static final String ALLOWLIST_KEY = "pipeline.telemetry.tracing.client-spans.allowlist";
    private static final boolean FORCE_CLIENT_SPANS = readForceClientSpans();
    private static final Set<String> ALLOWLIST = readAllowlist();

    private GrpcClientTracing() {
    }

    /**
     * Wrap a unary gRPC client call with an OpenTelemetry client span.
     *
     * @param service gRPC service name
     * @param method gRPC method name
     * @param uni the reactive result
     * @param <T> output type
     * @return instrumented Uni
     */
    public static <T> Uni<T> traceUnary(String service, String method, Uni<T> uni) {
        if (service == null || method == null) {
            return uni;
        }
        return Uni.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            Span span = startSpan(service, method);
            Scope scope = span.makeCurrent();
            return uni.onTermination().invoke((item, failure, cancelled) -> {
                recordClientMetrics(service, method, failure, startNanos);
                endSpan(span, failure);
                scope.close();
            });
        });
    }

    /**
     * Wrap a streaming gRPC client call with an OpenTelemetry client span.
     *
     * @param service gRPC service name
     * @param method gRPC method name
     * @param multi the reactive stream
     * @param <T> output type
     * @return instrumented Multi
     */
    public static <T> Multi<T> traceMulti(String service, String method, Multi<T> multi) {
        if (service == null || method == null) {
            return multi;
        }
        return Multi.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            Span span = startSpan(service, method);
            Scope scope = span.makeCurrent();
            AtomicReference<Throwable> failureRef = new AtomicReference<>();
            return multi.onFailure().invoke(failureRef::set)
                .onTermination().invoke(() -> {
                    recordClientMetrics(service, method, failureRef.get(), startNanos);
                    endSpan(span, failureRef.get());
                    scope.close();
                });
        });
    }

    private static Span startSpan(String service, String method) {
        var builder = tracer().spanBuilder(service + "/" + method)
            .setSpanKind(SpanKind.CLIENT)
            .setAttribute(RPC_SYSTEM, "grpc")
            .setAttribute(RPC_SERVICE, service)
            .setAttribute(RPC_METHOD, method)
            .setAttribute(PEER_SERVICE, service);
        if (shouldForceSample(service)) {
            builder.setParent(forceSampledParent());
        }
        return builder.startSpan();
    }

    private static Tracer tracer() {
        return GlobalOpenTelemetry.getTracer("org.pipelineframework.grpc.client");
    }

    private static void endSpan(Span span, Throwable failure) {
        if (span == null) {
            return;
        }
        Status.Code statusCode = Status.Code.OK;
        if (failure != null) {
            span.recordException(failure);
            span.setStatus(StatusCode.ERROR);
            statusCode = Status.fromThrowable(failure).getCode();
        }
        span.setAttribute(RPC_GRPC_STATUS, (long) statusCode.value());
        span.end();
    }

    private static void recordClientMetrics(String service, String method, Throwable failure, long startNanos) {
        Status.Code statusCode = Status.Code.OK;
        if (failure != null) {
            statusCode = Status.fromThrowable(failure).getCode();
        }
        RpcMetrics.recordGrpcClient(service, method, statusCode, System.nanoTime() - startNanos);
    }

    private static boolean shouldForceSample(String service) {
        if (!FORCE_CLIENT_SPANS) {
            return false;
        }
        if (ALLOWLIST.isEmpty()) {
            return true;
        }
        return ALLOWLIST.contains(service);
    }

    private static Context forceSampledParent() {
        SpanContext parent = sampledParentContext();
        return Context.root().with(Span.wrap(parent));
    }

    private static SpanContext sampledParentContext() {
        long high = ThreadLocalRandom.current().nextLong();
        long low = ThreadLocalRandom.current().nextLong();
        if (high == 0 && low == 0) {
            low = 1;
        }
        String traceId = TraceId.fromLongs(high, low);
        String spanId = SpanId.fromLong(ThreadLocalRandom.current().nextLong());
        return SpanContext.create(traceId, spanId, TraceFlags.getSampled(), TraceState.getDefault());
    }

    private static boolean readForceClientSpans() {
        try {
            return ConfigProvider.getConfig().getOptionalValue(FORCE_CLIENT_SPANS_KEY, Boolean.class).orElse(false);
        } catch (Exception ignored) {
            return false;
        }
    }

    private static Set<String> readAllowlist() {
        try {
            String raw = ConfigProvider.getConfig().getOptionalValue(ALLOWLIST_KEY, String.class).orElse("");
            if (raw.isBlank()) {
                return Collections.emptySet();
            }
            return Collections.unmodifiableSet(
                java.util.Arrays.stream(raw.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toSet()));
        } catch (Exception ignored) {
            return Collections.emptySet();
        }
    }
}
