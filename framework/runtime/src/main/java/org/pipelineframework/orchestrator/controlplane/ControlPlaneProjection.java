package org.pipelineframework.orchestrator.controlplane;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public record ControlPlaneProjection(
    String tenantId,
    String runId,
    long version,
    Optional<PipelineRun> run,
    Map<String, ExecutionSegment> segments,
    Map<String, SegmentAttempt> attempts,
    Map<String, BoundaryUnit> boundaries,
    Map<String, BoundaryInteraction> interactions,
    Set<String> terminalPublicationPreparedKeys,
    Set<String> terminalPublicationKeys,
    Set<String> factKeys
) {
    public ControlPlaneProjection {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        ControlPlaneChecks.requireNonNegative(version, "version");
        run = run == null ? Optional.empty() : run;
        segments = ControlPlaneChecks.copyMap(segments);
        attempts = ControlPlaneChecks.copyMap(attempts);
        boundaries = ControlPlaneChecks.copyMap(boundaries);
        interactions = ControlPlaneChecks.copyMap(interactions);
        terminalPublicationPreparedKeys = ControlPlaneChecks.copySet(terminalPublicationPreparedKeys);
        terminalPublicationKeys = ControlPlaneChecks.copySet(terminalPublicationKeys);
        factKeys = ControlPlaneChecks.copySet(factKeys);
    }

    public static ControlPlaneProjection empty(String tenantId, String runId) {
        return new ControlPlaneProjection(
            tenantId,
            runId,
            0L,
            Optional.empty(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Set.of(),
            Set.of(),
            Set.of());
    }

    public PipelineRunStatus status() {
        return run.map(PipelineRun::status).orElse(PipelineRunStatus.ACCEPTED);
    }
}
