/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

/** Thin static compatibility delegates for legacy retry/reject APIs. */
final class RetryObservationCompatibility {
    private RetryObservationCompatibility() { }

    private static RetryObservationRouter delegate() {
        return TelemetryCompatibilityAccess.adapter(RetryObservationRouter.class, RetryObservationRouter::new);
    }

    static void add(PipelineRetryTelemetry telemetry) { delegate().add(telemetry); }
    static void register(PipelineRunContext context, PipelineRetryTelemetry telemetry) {
        delegate().register(context, telemetry);
    }
    static void unregister(PipelineRunContext context, PipelineRetryTelemetry telemetry) {
        delegate().unregister(context, telemetry);
    }
    static void remove(PipelineRetryTelemetry telemetry) { delegate().remove(telemetry); }
    static void retry(Class<?> stepClass, Throwable failure) { delegate().retry(stepClass, failure); }
    static void reject(Class<?> stepClass, String scope, String errorType, String errorMessage) {
        delegate().reject(stepClass, scope, errorType, errorMessage);
    }
}
