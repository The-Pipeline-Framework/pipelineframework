/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.inject.Singleton;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/** Stateful compatibility router for legacy retry/reject calls that carry no explicit run context. */
@Singleton
final class RetryObservationRouter {
    private final Set<PipelineRetryTelemetry> active = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, PipelineRetryTelemetry> byTrace = new ConcurrentHashMap<>();
    private final AtomicReference<PipelineRetryTelemetry> lastCreated = new AtomicReference<>();

    void add(PipelineRetryTelemetry telemetry) {
        active.add(telemetry);
        lastCreated.set(telemetry);
    }

    void register(PipelineRunContext context, PipelineRetryTelemetry telemetry) {
        if (context != null && context.span() != null && context.span().getSpanContext().isValid()) {
            byTrace.put(context.span().getSpanContext().getTraceId(), telemetry);
        }
    }

    void unregister(PipelineRunContext context, PipelineRetryTelemetry telemetry) {
        if (context != null && context.span() != null && context.span().getSpanContext().isValid()) {
            byTrace.remove(context.span().getSpanContext().getTraceId(), telemetry);
        }
    }

    void remove(PipelineRetryTelemetry telemetry) {
        active.remove(telemetry);
        lastCreated.compareAndSet(telemetry, null);
        byTrace.values().removeIf(current -> current == telemetry);
    }

    void retry(Class<?> stepClass, Throwable failure) {
        PipelineRetryTelemetry telemetry = current();
        if (telemetry != null) {
            telemetry.retry(stepClass, failure);
        }
    }

    void reject(Class<?> stepClass, String scope, String errorType, String errorMessage) {
        PipelineRetryTelemetry telemetry = current();
        if (telemetry != null) {
            telemetry.reject(stepClass, scope, errorType, errorMessage);
        }
    }

    private PipelineRetryTelemetry current() {
        SpanContext context = Span.current().getSpanContext();
        if (context.isValid()) {
            PipelineRetryTelemetry matching = byTrace.get(context.getTraceId());
            if (matching != null) {
                return matching;
            }
        }
        return active.size() == 1 ? active.iterator().next() : lastCreated.get();
    }
}
