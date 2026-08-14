/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry.observation;

import java.time.Instant;
import java.util.Objects;

/** Immutable semantic facts for one subscribed pipeline-step execution. */
public sealed interface StepObservation permits StepObservation.Started, StepObservation.Completed,
    StepObservation.Failed, StepObservation.Cancelled {

    Context context();
    Instant occurredAt();

    record Context(String stepClass, boolean perItem) {
        public Context {
            Objects.requireNonNull(stepClass, "stepClass");
        }
    }

    record Started(Context context, Instant occurredAt) implements StepObservation {
        public Started { require(context, occurredAt); }
    }

    record Completed(Context context, long durationNanos, Instant occurredAt) implements StepObservation {
        public Completed { require(context, occurredAt); durationNanos = nonNegative(durationNanos); }
    }

    record Failed(Context context, long durationNanos, Throwable failure, Instant occurredAt)
        implements StepObservation {
        public Failed {
            require(context, occurredAt);
            Objects.requireNonNull(failure, "failure");
            durationNanos = nonNegative(durationNanos);
        }
    }

    record Cancelled(Context context, long durationNanos, Instant occurredAt) implements StepObservation {
        public Cancelled { require(context, occurredAt); durationNanos = nonNegative(durationNanos); }
    }

    private static void require(Context context, Instant occurredAt) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }

    private static long nonNegative(long durationNanos) {
        return Math.max(0L, durationNanos);
    }
}
