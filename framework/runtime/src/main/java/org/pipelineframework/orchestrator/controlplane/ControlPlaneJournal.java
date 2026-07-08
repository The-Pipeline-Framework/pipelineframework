package org.pipelineframework.orchestrator.controlplane;

import java.util.List;
import io.smallrye.mutiny.Uni;

public interface ControlPlaneJournal {

    Uni<ControlPlaneAppendResult> append(
        String tenantId,
        String runId,
        long expectedVersion,
        List<ControlPlaneFact> facts,
        long nowEpochMs);

    Uni<ControlPlaneProjection> projection(String tenantId, String runId);

    Uni<List<DueSegment>> findDueSegments(long nowEpochMs, int limit);

    Uni<List<DueBoundaryInteraction>> findTimedOutInteractions(long nowEpochMs, int limit);
}
