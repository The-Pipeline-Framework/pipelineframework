/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.orchestrator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.telemetry.NoopTelemetryRuntime;
import org.pipelineframework.telemetry.TelemetryRuntime;
import org.pipelineframework.telemetry.TelemetrySdkAttributes;
import org.pipelineframework.telemetry.derivation.TransitionTelemetryDerivation;

/** Effectful transition-worker span adapter. */
@ApplicationScoped
final class TransitionWorkerTracing {
    private final TelemetryRuntime runtime;

    @Inject
    TransitionWorkerTracing(TelemetryRuntime runtime) {
        this.runtime = runtime;
    }

    static TransitionWorkerTracing disabled() {
        return new TransitionWorkerTracing(new NoopTelemetryRuntime());
    }

    void record(TransitionTelemetryDerivation.SpanPlan plan) {
        var span = runtime.tracer("org.pipelineframework.orchestrator").spanBuilder(plan.name())
            .setAllAttributes(TelemetrySdkAttributes.from(plan.attributes())).startSpan();
        span.end();
    }
}
