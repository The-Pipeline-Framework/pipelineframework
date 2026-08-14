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

/** Immutable queue-async transition-worker facts. */
public sealed interface TransitionObservation permits TransitionObservation.Admitted,
    TransitionObservation.Released, TransitionObservation.Saturated, TransitionObservation.Dispatched,
    TransitionObservation.OutcomeRecorded, TransitionObservation.DurationRecorded {

    Instant occurredAt();

    record Admitted(Instant occurredAt) implements TransitionObservation { public Admitted { require(occurredAt); } }
    record Released(Instant occurredAt) implements TransitionObservation { public Released { require(occurredAt); } }
    record Saturated(Instant occurredAt) implements TransitionObservation { public Saturated { require(occurredAt); } }
    record Dispatched(Instant occurredAt) implements TransitionObservation { public Dispatched { require(occurredAt); } }
    record OutcomeRecorded(String outcome, Instant occurredAt) implements TransitionObservation {
        public OutcomeRecorded {
            Objects.requireNonNull(outcome, "outcome");
            require(occurredAt);
        }
    }
    record DurationRecorded(long durationNanos, Instant occurredAt) implements TransitionObservation {
        public DurationRecorded {
            durationNanos = Math.max(0L, durationNanos);
            require(occurredAt);
        }
    }

    private static void require(Instant value) { Objects.requireNonNull(value, "occurredAt"); }
}
