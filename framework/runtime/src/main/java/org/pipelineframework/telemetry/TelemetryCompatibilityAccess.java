/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.pipelineframework.runtime.core.RuntimeAdapters;

/** Thin compatibility lookup for legacy public static telemetry APIs. */
public final class TelemetryCompatibilityAccess {
    private static final TelemetryRuntime NOOP = new NoopTelemetryRuntime();
    private static final ConcurrentMap<Class<?>, Object> FALLBACK_ADAPTERS = new ConcurrentHashMap<>();

    private TelemetryCompatibilityAccess() {
    }

    public static TelemetryRuntime metricsRuntime() {
        return policy().map(TelemetryPolicy::metricsEnabled).filter(Boolean.TRUE::equals).isPresent()
            ? runtime()
            : policy().isPresent() ? NOOP : TelemetryRuntimes.global();
    }

    public static TelemetryRuntime tracingRuntime() {
        return policy().map(TelemetryPolicy::tracingEnabled).filter(Boolean.TRUE::equals).isPresent()
            ? runtime()
            : policy().isPresent() ? NOOP : TelemetryRuntimes.global();
    }

    public static TelemetryRuntime runtime() {
        return RuntimeAdapters.resolveBean(TelemetryRuntime.class).orElseGet(TelemetryRuntimes::global);
    }

    /** Resolve a focused CDI adapter, retaining a deterministic compatibility instance outside a container. */
    public static <T> T adapter(Class<T> type, Supplier<T> fallback) {
        return RuntimeAdapters.resolveBean(type).orElseGet(() -> type.cast(
            FALLBACK_ADAPTERS.computeIfAbsent(type, ignored -> fallback.get())));
    }

    private static java.util.Optional<TelemetryPolicy> policy() {
        return RuntimeAdapters.resolveBean(TelemetryPolicySource.class).map(TelemetryPolicySource::telemetryPolicy);
    }
}
