package org.pipelineframework.orchestrator.controlplane;

import java.util.List;
import java.util.Objects;

public record ControlPlaneAppendResult(
    ControlPlaneProjection projection,
    List<ControlPlaneEvent> appendedEvents
) {
    public ControlPlaneAppendResult {
        Objects.requireNonNull(projection, "projection must not be null");
        appendedEvents = ControlPlaneChecks.copyList(appendedEvents);
    }
}
