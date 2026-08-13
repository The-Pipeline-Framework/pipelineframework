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

import io.grpc.Status;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

/**
 * Records OpenTelemetry RPC server metrics for gRPC requests.
 */
@Singleton
final class RpcMetricsRecorder {

    private final AttributeKey<String> RPC_SYSTEM = AttributeKey.stringKey("rpc.system");
    private final AttributeKey<String> RPC_SERVICE = AttributeKey.stringKey("rpc.service");
    private final AttributeKey<String> RPC_METHOD = AttributeKey.stringKey("rpc.method");
    private final AttributeKey<Long> RPC_GRPC_STATUS = AttributeKey.longKey("rpc.grpc.status_code");

    RpcMetricsRecorder() {
    }

    /**
     * Record gRPC server RPC metrics for a completed call.
     *
     * @param service gRPC service name
     * @param method gRPC method name
     * @param code gRPC status code
     * @param durationNanos duration in nanoseconds
     */
    public void recordGrpcServer(String service, String method, Status.Code code, long durationNanos) {
        if (service == null || method == null) {
            return;
        }
        Instruments instruments = instruments();
        Status.Code resolved = code == null ? Status.Code.UNKNOWN : code;
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = Attributes.builder()
            .put(RPC_SYSTEM, "grpc")
            .put(RPC_SERVICE, service)
            .put(RPC_METHOD, method)
            .put(RPC_GRPC_STATUS, (long) resolved.value())
            .build();
        instruments.serverRequests().add(1, attributes);
        instruments.serverResponses().add(1, attributes);
        instruments.serverDuration().record(durationMs, attributes);
        instruments.sloServerTotal().add(1, attributes);
        if (resolved == Status.Code.OK) {
            instruments.sloServerGood().add(1, attributes);
        }
        instruments.sloServerLatencyTotal().add(1, attributes);
        if (resolved == Status.Code.OK && durationMs <= thresholdMs) {
            instruments.sloServerLatencyGood().add(1, attributes);
        }
    }

    /**
     * Record gRPC server RPC metrics for a completed call.
     *
     * @param service gRPC service name
     * @param method gRPC method name
     * @param status gRPC status
     * @param durationNanos duration in nanoseconds
     */
    public void recordGrpcServer(String service, String method, Status status, long durationNanos) {
        Status.Code code = status == null ? Status.Code.UNKNOWN : status.getCode();
        recordGrpcServer(service, method, code, durationNanos);
    }

    /**
     * Record gRPC client RPC metrics for a completed call.
     *
     * @param service gRPC service name
     * @param method gRPC method name
     * @param code gRPC status code
     * @param durationNanos duration in nanoseconds
     */
    public void recordGrpcClient(String service, String method, Status.Code code, long durationNanos) {
        if (service == null || method == null) {
            return;
        }
        Instruments instruments = instruments();
        Status.Code resolved = code == null ? Status.Code.UNKNOWN : code;
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = Attributes.builder()
            .put(RPC_SYSTEM, "grpc")
            .put(RPC_SERVICE, service)
            .put(RPC_METHOD, method)
            .put(RPC_GRPC_STATUS, (long) resolved.value())
            .build();
        instruments.sloClientTotal().add(1, attributes);
        if (resolved == Status.Code.OK) {
            instruments.sloClientGood().add(1, attributes);
        }
        instruments.sloClientLatencyTotal().add(1, attributes);
        if (resolved == Status.Code.OK && durationMs <= thresholdMs) {
            instruments.sloClientLatencyGood().add(1, attributes);
        }
    }

    /**
     * Record gRPC client RPC metrics for a completed call.
     *
     * @param service gRPC service name
     * @param method gRPC method name
     * @param status gRPC status
     * @param durationNanos duration in nanoseconds
     */
    public void recordGrpcClient(String service, String method, Status status, long durationNanos) {
        Status.Code code = status == null ? Status.Code.UNKNOWN : status.getCode();
        recordGrpcClient(service, method, code, durationNanos);
    }

    private Instruments instruments() {
        Meter meter = TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework.rpc");
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
