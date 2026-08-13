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
import java.util.function.Function;

/** Compatibility delegates for the focused gRPC tracing adapter. */
public final class GrpcClientTracing {
    private GrpcClientTracing() { }

    private static GrpcClientTracingRecorder delegate() {
        return TelemetryCompatibilityAccess.adapter(GrpcClientTracingRecorder.class, GrpcClientTracingRecorder::new);
    }

    public static <T> Uni<T> traceUnary(String service, String method, Uni<T> uni) {
        return delegate().traceUnary(service, method, uni);
    }

    public static <T> Multi<T> traceMulti(String service, String method, Multi<T> multi) {
        return delegate().traceMulti(service, method, multi);
    }

    public static <I, O> Uni<O> traceUnaryFromStream(
        String service, String method, Multi<I> request, Function<Multi<I>, Uni<O>> invocation
    ) {
        return delegate().traceUnaryFromStream(service, method, request, invocation);
    }

    public static <I, O> Multi<O> traceMultiFromStream(
        String service, String method, Multi<I> request, Function<Multi<I>, Multi<O>> invocation
    ) {
        return delegate().traceMultiFromStream(service, method, request, invocation);
    }
}
