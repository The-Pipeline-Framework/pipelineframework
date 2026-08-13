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

/** Immutable execution-rejection fact. */
public record RejectObservation(String stepClass, String scope, String errorType,
                                String errorMessage, Instant occurredAt) {
    public RejectObservation {
        Objects.requireNonNull(stepClass, "stepClass");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }
}
