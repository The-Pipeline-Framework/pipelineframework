/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.awaitable;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.telemetry.AwaitObservation;

/** Pure conversion from Await runtime records to canonical observation values. */
final class AwaitObservations {
    private AwaitObservations() { }

    static AwaitObservation.Context context(Optional<AwaitInteractionRecord> record,
                                            Optional<AwaitUnitRecord> unit) {
        return new AwaitObservation.Context(
            record.map(AwaitInteractionRecord::stepId).or(() -> unit.map(AwaitUnitRecord::stepId)),
            record.map(AwaitInteractionRecord::transportType),
            record.map(AwaitInteractionRecord::status).map(Enum::name)
                .or(() -> unit.map(AwaitUnitRecord::status).map(Enum::name)),
            unit.map(AwaitUnitRecord::cardinality),
            record.map(AwaitInteractionRecord::executionId),
            record.map(AwaitInteractionRecord::interactionId),
            record.map(AwaitInteractionRecord::correlationId),
            record.map(AwaitInteractionRecord::unitId).or(() -> unit.map(AwaitUnitRecord::unitId)),
            record.map(AwaitInteractionRecord::transportMetadata).orElse(Map.of()));
    }

    static AwaitObservation.CompletionAdmitted completionAdmitted(AwaitInteractionRecord record, Instant now) {
        long latency = record.createdAtEpochMs() > 0 && record.updatedAtEpochMs() >= record.createdAtEpochMs()
            ? record.updatedAtEpochMs() - record.createdAtEpochMs() : 0L;
        return new AwaitObservation.CompletionAdmitted(context(Optional.of(record), Optional.empty()), latency, now);
    }

    static AwaitObservation.UnitTerminal unitTerminal(AwaitInteractionRecord record, AwaitUnitRecord unit, Instant now) {
        long duration = unit.createdAtEpochMs() > 0 && unit.updatedAtEpochMs() >= unit.createdAtEpochMs()
            ? unit.updatedAtEpochMs() - unit.createdAtEpochMs() : 0L;
        return new AwaitObservation.UnitTerminal(context(Optional.of(record), Optional.of(unit)), duration, now);
    }

    static AwaitObservation.Context emptyContext() {
        return context(Optional.empty(), Optional.empty());
    }
}
