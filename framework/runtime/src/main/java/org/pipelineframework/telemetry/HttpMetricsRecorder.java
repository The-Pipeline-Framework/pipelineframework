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

import jakarta.inject.Singleton;

import java.util.concurrent.CancellationException;
import jakarta.ws.rs.WebApplicationException;

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
@Singleton
final class HttpMetricsRecorder {

    private final AttributeKey<String> RPC_SYSTEM = AttributeKey.stringKey("rpc.system");
    private final AttributeKey<String> RPC_SERVICE = AttributeKey.stringKey("rpc.service");
    private final AttributeKey<String> RPC_METHOD = AttributeKey.stringKey("rpc.method");
    private final AttributeKey<Long> HTTP_STATUS = AttributeKey.longKey("http.status_code");

    HttpMetricsRecorder() {
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
    public <T> Uni<T> instrumentClient(String service, String method, Uni<T> uni) {
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
    public <T> Multi<T> instrumentClient(String service, String method, Multi<T> multi) {
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
    public void recordHttpServer(String service, String method, Throwable failure, long startNanos) {
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
    public void recordHttpClient(String service, String method, Throwable failure, long startNanos) {
        int status = resolveStatus(failure);
        recordHttpClient(service, method, status, System.nanoTime() - startNanos);
    }

    private void recordHttpServer(String service, String method, int statusCode, long durationNanos) {
        if (service == null || method == null) {
            return;
        }
        Instruments instruments = instruments();
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = buildAttributes(service, method, statusCode);
        instruments.serverRequests().add(1, attributes);
        instruments.serverResponses().add(1, attributes);
        instruments.serverDuration().record(durationMs, attributes);
        instruments.sloServerTotal().add(1, attributes);
        if (statusCode < 400) {
            instruments.sloServerGood().add(1, attributes);
        }
        instruments.sloServerLatencyTotal().add(1, attributes);
        if (statusCode < 400 && durationMs <= thresholdMs) {
            instruments.sloServerLatencyGood().add(1, attributes);
        }
    }

    private void recordHttpClient(String service, String method, int statusCode, long durationNanos) {
        if (service == null || method == null) {
            return;
        }
        Instruments instruments = instruments();
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = buildAttributes(service, method, statusCode);
        instruments.sloClientTotal().add(1, attributes);
        if (statusCode < 400) {
            instruments.sloClientGood().add(1, attributes);
        }
        instruments.sloClientLatencyTotal().add(1, attributes);
        if (statusCode < 400 && durationMs <= thresholdMs) {
            instruments.sloClientLatencyGood().add(1, attributes);
        }
    }

    private int resolveStatus(Throwable failure) {
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

    private Attributes buildAttributes(String service, String method, int statusCode) {
        return Attributes.builder()
            .put(RPC_SYSTEM, "http")
            .put(RPC_SERVICE, service)
            .put(RPC_METHOD, method)
            .put(HTTP_STATUS, (long) statusCode)
            .build();
    }

    private Instruments instruments() {
        Meter meter = TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework.http");
        return new Instruments(
            meter.counterBuilder("rpc.server.requests").build(), meter.counterBuilder("rpc.server.responses").build(),
            meter.histogramBuilder("rpc.server.duration").setUnit("ms").build(),
            meter.counterBuilder("tpf.slo.rpc.server.total").build(), meter.counterBuilder("tpf.slo.rpc.server.good").build(),
            meter.counterBuilder("tpf.slo.rpc.server.latency.total").build(), meter.counterBuilder("tpf.slo.rpc.server.latency.good").build(),
            meter.counterBuilder("tpf.slo.rpc.client.total").build(), meter.counterBuilder("tpf.slo.rpc.client.good").build(),
            meter.counterBuilder("tpf.slo.rpc.client.latency.total").build(), meter.counterBuilder("tpf.slo.rpc.client.latency.good").build());
    }

    private record Instruments(LongCounter serverRequests, LongCounter serverResponses, DoubleHistogram serverDuration,
        LongCounter sloServerTotal, LongCounter sloServerGood, LongCounter sloServerLatencyTotal,
        LongCounter sloServerLatencyGood, LongCounter sloClientTotal, LongCounter sloClientGood,
        LongCounter sloClientLatencyTotal, LongCounter sloClientLatencyGood) { }
}
