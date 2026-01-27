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

import java.util.concurrent.CancellationException;
import jakarta.ws.rs.WebApplicationException;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Records OpenTelemetry HTTP metrics for REST calls using SLO-friendly counters.
 */
public final class HttpMetrics {

    private static volatile Meter meter;
    private static volatile LongCounter serverRequests;
    private static volatile LongCounter serverResponses;
    private static volatile DoubleHistogram serverDuration;
    private static volatile LongCounter sloServerTotal;
    private static volatile LongCounter sloServerGood;
    private static volatile LongCounter sloServerLatencyTotal;
    private static volatile LongCounter sloServerLatencyGood;
    private static volatile LongCounter sloClientTotal;
    private static volatile LongCounter sloClientGood;
    private static volatile LongCounter sloClientLatencyTotal;
    private static volatile LongCounter sloClientLatencyGood;

    private static final AttributeKey<String> RPC_SYSTEM = AttributeKey.stringKey("rpc.system");
    private static final AttributeKey<String> RPC_SERVICE = AttributeKey.stringKey("rpc.service");
    private static final AttributeKey<String> RPC_METHOD = AttributeKey.stringKey("rpc.method");
    private static final AttributeKey<Long> HTTP_STATUS = AttributeKey.longKey("http.status_code");

    private HttpMetrics() {
    }

    /**
     * Wrap a REST client call with SLO-ready counters.
     *
     * @param service service name
     * @param method method name
     * @param uni client result
     * @param <T> output type
     * @return instrumented Uni
     */
    public static <T> Uni<T> instrumentClient(String service, String method, Uni<T> uni) {
        if (service == null || method == null) {
            return uni;
        }
        return Uni.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            return uni.onTermination().invoke((item, failure, cancelled) -> {
                Throwable resolved = cancelled ? new CancellationException("HTTP client call cancelled") : failure;
                recordHttpClient(service, method, resolved, startNanos);
            });
        });
    }

    /**
     * Wrap a streaming REST client call with SLO-ready counters.
     *
     * @param service service name
     * @param method method name
     * @param multi client result
     * @param <T> output type
     * @return instrumented Multi
     */
    public static <T> Multi<T> instrumentClient(String service, String method, Multi<T> multi) {
        if (service == null || method == null) {
            return multi;
        }
        return Multi.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            return multi.onTermination().invoke((failure, cancelled) -> {
                Throwable resolved = cancelled ? new CancellationException("HTTP client call cancelled") : failure;
                recordHttpClient(service, method, resolved, startNanos);
            });
        });
    }

    /**
     * Record REST server metrics for a completed call.
     *
     * @param service service name
     * @param method method name
     * @param failure failure if any
     * @param startNanos start timestamp in nanoseconds
     */
    public static void recordHttpServer(String service, String method, Throwable failure, long startNanos) {
        int status = resolveStatus(failure);
        recordHttpServer(service, method, status, System.nanoTime() - startNanos);
    }

    /**
     * Record REST client metrics for a completed call.
     *
     * @param service service name
     * @param method method name
     * @param failure failure if any
     * @param startNanos start timestamp in nanoseconds
     */
    public static void recordHttpClient(String service, String method, Throwable failure, long startNanos) {
        int status = resolveStatus(failure);
        recordHttpClient(service, method, status, System.nanoTime() - startNanos);
    }

    private static void recordHttpServer(String service, String method, int statusCode, long durationNanos) {
        if (service == null || method == null) {
            return;
        }
        ensureInitialized();
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = buildAttributes(service, method, statusCode);
        serverRequests.add(1, attributes);
        serverResponses.add(1, attributes);
        serverDuration.record(durationMs, attributes);
        sloServerTotal.add(1, attributes);
        if (statusCode < 400) {
            sloServerGood.add(1, attributes);
        }
        sloServerLatencyTotal.add(1, attributes);
        if (statusCode < 400 && durationMs <= thresholdMs) {
            sloServerLatencyGood.add(1, attributes);
        }
    }

    private static void recordHttpClient(String service, String method, int statusCode, long durationNanos) {
        if (service == null || method == null) {
            return;
        }
        ensureInitialized();
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = buildAttributes(service, method, statusCode);
        sloClientTotal.add(1, attributes);
        if (statusCode < 400) {
            sloClientGood.add(1, attributes);
        }
        sloClientLatencyTotal.add(1, attributes);
        if (statusCode < 400 && durationMs <= thresholdMs) {
            sloClientLatencyGood.add(1, attributes);
        }
    }

    private static int resolveStatus(Throwable failure) {
        if (failure == null) {
            return 200;
        }
        if (failure instanceof CancellationException) {
            return 499;
        }
        if (failure instanceof WebApplicationException web) {
            return web.getResponse() != null ? web.getResponse().getStatus() : 500;
        }
        return 500;
    }

    private static Attributes buildAttributes(String service, String method, int statusCode) {
        return Attributes.builder()
            .put(RPC_SYSTEM, "http")
            .put(RPC_SERVICE, service)
            .put(RPC_METHOD, method)
            .put(HTTP_STATUS, (long) statusCode)
            .build();
    }

    private static void ensureInitialized() {
        if (meter != null) {
            return;
        }
        synchronized (HttpMetrics.class) {
            if (meter != null) {
                return;
            }
            Meter localMeter = GlobalOpenTelemetry.getMeter("org.pipelineframework.http");
            serverRequests = localMeter.counterBuilder("rpc.server.requests").build();
            serverResponses = localMeter.counterBuilder("rpc.server.responses").build();
            serverDuration = localMeter.histogramBuilder("rpc.server.duration").setUnit("ms").build();
            sloServerTotal = localMeter.counterBuilder("tpf.slo.rpc.server.total").build();
            sloServerGood = localMeter.counterBuilder("tpf.slo.rpc.server.good").build();
            sloServerLatencyTotal = localMeter.counterBuilder("tpf.slo.rpc.server.latency.total").build();
            sloServerLatencyGood = localMeter.counterBuilder("tpf.slo.rpc.server.latency.good").build();
            sloClientTotal = localMeter.counterBuilder("tpf.slo.rpc.client.total").build();
            sloClientGood = localMeter.counterBuilder("tpf.slo.rpc.client.good").build();
            sloClientLatencyTotal = localMeter.counterBuilder("tpf.slo.rpc.client.latency.total").build();
            sloClientLatencyGood = localMeter.counterBuilder("tpf.slo.rpc.client.latency.good").build();
            meter = localMeter;
        }
    }

    public static synchronized void resetForTest() {
        // Test-only helper to clear cached meters/counters.
        meter = null;
        serverRequests = null;
        serverResponses = null;
        serverDuration = null;
        sloServerTotal = null;
        sloServerGood = null;
        sloServerLatencyTotal = null;
        sloServerLatencyGood = null;
        sloClientTotal = null;
        sloClientGood = null;
        sloClientLatencyTotal = null;
        sloClientLatencyGood = null;
    }
}
