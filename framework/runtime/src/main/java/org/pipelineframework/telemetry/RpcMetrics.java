/*
 * Copyright (c) 2023-2025 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import io.grpc.Status;

/** Compatibility delegates for the focused RPC metrics adapter. */
public final class RpcMetrics {
    private RpcMetrics() { }

    private static RpcMetricsRecorder delegate() {
        return TelemetryCompatibilityAccess.adapter(RpcMetricsRecorder.class, RpcMetricsRecorder::new);
    }

    public static void recordGrpcServer(String service, String method, Status.Code code, long durationNanos) {
        delegate().recordGrpcServer(service, method, code, durationNanos);
    }

    public static void recordGrpcServer(String service, String method, Status status, long durationNanos) {
        delegate().recordGrpcServer(service, method, status, durationNanos);
    }

    public static void recordGrpcClient(String service, String method, Status.Code code, long durationNanos) {
        delegate().recordGrpcClient(service, method, code, durationNanos);
    }

    public static void recordGrpcClient(String service, String method, Status status, long durationNanos) {
        delegate().recordGrpcClient(service, method, status, durationNanos);
    }
}
