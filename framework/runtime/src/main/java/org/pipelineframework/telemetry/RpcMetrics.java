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
        ensureInitialized();
        Status.Code resolved = code == null ? Status.Code.UNKNOWN : code;
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = Attributes.builder()
            .put(RPC_SYSTEM, "grpc")
            .put(RPC_SERVICE, service)
            .put(RPC_METHOD, method)
            .put(RPC_GRPC_STATUS, (long) resolved.value())
            .build();
        serverRequests.add(1, attributes);
        serverResponses.add(1, attributes);
        serverDuration.record(durationMs, attributes);
        sloServerTotal.add(1, attributes);
        if (resolved == Status.Code.OK) {
            sloServerGood.add(1, attributes);
        }
        sloServerLatencyTotal.add(1, attributes);
        if (resolved == Status.Code.OK && durationMs <= thresholdMs) {
            sloServerLatencyGood.add(1, attributes);
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
        ensureInitialized();
        Status.Code resolved = code == null ? Status.Code.UNKNOWN : code;
        double durationMs = durationNanos / 1_000_000.0;
        double thresholdMs = TelemetrySloConfig.rpcLatencyMs();
        Attributes attributes = Attributes.builder()
            .put(RPC_SYSTEM, "grpc")
            .put(RPC_SERVICE, service)
            .put(RPC_METHOD, method)
            .put(RPC_GRPC_STATUS, (long) resolved.value())
            .build();
        sloClientTotal.add(1, attributes);
        if (resolved == Status.Code.OK) {
            sloClientGood.add(1, attributes);
        }
        sloClientLatencyTotal.add(1, attributes);
        if (resolved == Status.Code.OK && durationMs <= thresholdMs) {
            sloClientLatencyGood.add(1, attributes);
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

    static void resetForTest() {
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

    private static void ensureInitialized() {
        if (meter != null) {
            return;
        }
        synchronized (RpcMetrics.class) {
            if (meter != null) {
                return;
            }
            Meter localMeter = GlobalOpenTelemetry.getMeter("org.pipelineframework.rpc");
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
}
