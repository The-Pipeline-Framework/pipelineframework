package org.pipelineframework.orchestrator.controlplane;

import java.util.Objects;

public record ControlPlaneEvent(
    long sequence,
    long occurredAtEpochMs,
    ControlPlaneFact fact
) {
    public ControlPlaneEvent {
        if (sequence <= 0) {
            throw new IllegalArgumentException("sequence must be positive");
        }
        ControlPlaneChecks.requireNonNegative(occurredAtEpochMs, "occurredAtEpochMs");
        Objects.requireNonNull(fact, "fact must not be null");
    }
}
