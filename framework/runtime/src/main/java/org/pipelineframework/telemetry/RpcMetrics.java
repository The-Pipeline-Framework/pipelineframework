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

import io.grpc.Status;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

/**
 * Records OpenTelemetry RPC server metrics for gRPC requests.
 */
public final class RpcMetrics {

    private static final Meter METER = GlobalOpenTelemetry.getMeter("org.pipelineframework.rpc");
    private static final LongCounter SERVER_REQUESTS =
        METER.counterBuilder("rpc.server.requests").build();
    private static final LongCounter SERVER_RESPONSES =
        METER.counterBuilder("rpc.server.responses").build();
    private static final DoubleHistogram SERVER_DURATION =
        METER.histogramBuilder("rpc.server.duration").setUnit("ms").build();
    private static final LongCounter SLO_SERVER_TOTAL =
        METER.counterBuilder("tpf.slo.rpc.server.total").build();
    private static final LongCounter SLO_SERVER_GOOD =
        METER.counterBuilder("tpf.slo.rpc.server.good").build();
    private static final LongCounter SLO_SERVER_LATENCY_TOTAL =
        METER.counterBuilder("tpf.slo.rpc.server.latency.total").build();
    private static final LongCounter SLO_SERVER_LATENCY_GOOD =
        METER.counterBuilder("tpf.slo.rpc.server.latency.good").build();
    private static final LongCounter SLO_CLIENT_TOTAL =
        METER.counterBuilder("tpf.slo.rpc.client.total").build();
    private static final LongCounter SLO_CLIENT_GOOD =
        METER.counterBuilder("tpf.slo.rpc.client.good").build();
    private static final LongCounter SLO_CLIENT_LATENCY_TOTAL =
        METER.counterBuilder("tpf.slo.rpc.client.latency.total").build();
    private static final LongCounter SLO_CLIENT_LATENCY_GOOD =
        METER.counterBuilder("tpf.slo.rpc.client.latency.good").build();

    private static final AttributeKey<String> RPC_SYSTEM = AttributeKey.stringKey("rpc.system");
    private static final AttributeKey<String> RPC_SERVICE = AttributeKey.stringKey("rpc.service");
    private static final AttributeKey<String> RPC_METHOD = AttributeKey.stringKey("rpc.method");
    private static final AttributeKey<Long> RPC_GRPC_STATUS = AttributeKey.longKey("rpc.grpc.status_code");

    private RpcMetrics() {
    }

    /**
     * Record gRPC server RPC metrics for a completed call.
     *
     * @param service gRPC service name
     * @param method gRPC method name
     * @param code gRPC status code
     * @param durationNanos duration in nanoseconds
     */
    public static void recordGrpcServer(String service, String method, Status.Code code, long durationNanos) {
        if (service == null || method == null) {
            return;
        }
        Status.Code resolved = code == null ? Status.Code.UNKNOWN : code;
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = Attributes.builder()
            .put(RPC_SYSTEM, "grpc")
            .put(RPC_SERVICE, service)
            .put(RPC_METHOD, method)
            .put(RPC_GRPC_STATUS, (long) resolved.value())
            .build();
        SERVER_REQUESTS.add(1, attributes);
        SERVER_RESPONSES.add(1, attributes);
        SERVER_DURATION.record(durationMs, attributes);
        SLO_SERVER_TOTAL.add(1, attributes);
        if (resolved == Status.Code.OK) {
            SLO_SERVER_GOOD.add(1, attributes);
        }
        SLO_SERVER_LATENCY_TOTAL.add(1, attributes);
        if (resolved == Status.Code.OK && durationMs <= thresholdMs) {
            SLO_SERVER_LATENCY_GOOD.add(1, attributes);
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
    public static void recordGrpcServer(String service, String method, Status status, long durationNanos) {
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
    public static void recordGrpcClient(String service, String method, Status.Code code, long durationNanos) {
        if (service == null || method == null) {
            return;
        }
        Status.Code resolved = code == null ? Status.Code.UNKNOWN : code;
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = Attributes.builder()
            .put(RPC_SYSTEM, "grpc")
            .put(RPC_SERVICE, service)
            .put(RPC_METHOD, method)
            .put(RPC_GRPC_STATUS, (long) resolved.value())
            .build();
        SLO_CLIENT_TOTAL.add(1, attributes);
        if (resolved == Status.Code.OK) {
            SLO_CLIENT_GOOD.add(1, attributes);
        }
        SLO_CLIENT_LATENCY_TOTAL.add(1, attributes);
        if (resolved == Status.Code.OK && durationMs <= thresholdMs) {
            SLO_CLIENT_LATENCY_GOOD.add(1, attributes);
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
    public static void recordGrpcClient(String service, String method, Status status, long durationNanos) {
        Status.Code code = status == null ? Status.Code.UNKNOWN : status.getCode();
        recordGrpcClient(service, method, code, durationNanos);
    }
}
