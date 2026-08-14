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

/** Immutable semantic facts for one pipeline-run lifecycle. */
public sealed interface RunObservation permits RunObservation.Started, RunObservation.Completed,
    RunObservation.Failed, RunObservation.Cancelled {

    String runId();
    Instant occurredAt();

    record Started(String runId, String inputKind, int stepCount, String parallelism,
                   int maxConcurrency, Instant occurredAt) implements RunObservation {
        public Started {
            require(runId, occurredAt);
            Objects.requireNonNull(inputKind, "inputKind");
            Objects.requireNonNull(parallelism, "parallelism");
        }
    }

    record Completed(String runId, long durationMillis, Instant occurredAt) implements RunObservation {
        public Completed { require(runId, occurredAt); durationMillis = Math.max(0L, durationMillis); }
    }

    record Failed(String runId, long durationMillis, Throwable failure, Instant occurredAt)
        implements RunObservation {
        public Failed {
            require(runId, occurredAt);
            Objects.requireNonNull(failure, "failure");
            durationMillis = Math.max(0L, durationMillis);
        }
    }

    record Cancelled(String runId, long durationMillis, Instant occurredAt) implements RunObservation {
        public Cancelled { require(runId, occurredAt); durationMillis = Math.max(0L, durationMillis); }
    }

    private static void require(String runId, Instant occurredAt) {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }
}
