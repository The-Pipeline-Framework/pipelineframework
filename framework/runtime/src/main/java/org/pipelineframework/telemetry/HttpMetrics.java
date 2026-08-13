/*
 * Copyright (c) 2023-2025 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/** Compatibility delegates for the focused HTTP metrics adapter. */
public final class HttpMetrics {
    private HttpMetrics() { }

    private static HttpMetricsRecorder delegate() {
        return TelemetryCompatibilityAccess.adapter(HttpMetricsRecorder.class, HttpMetricsRecorder::new);
    }

    public static <T> Uni<T> instrumentClient(String service, String method, Uni<T> uni) {
        return delegate().instrumentClient(service, method, uni);
    }

    public static <T> Multi<T> instrumentClient(String service, String method, Multi<T> multi) {
        return delegate().instrumentClient(service, method, multi);
    }

    public static void recordHttpServer(String service, String method, Throwable failure, long startNanos) {
        delegate().recordHttpServer(service, method, failure, startNanos);
    }

    public static void recordHttpClient(String service, String method, Throwable failure, long startNanos) {
        delegate().recordHttpClient(service, method, failure, startNanos);
    }
}
