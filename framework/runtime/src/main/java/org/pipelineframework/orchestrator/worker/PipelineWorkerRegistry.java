package org.pipelineframework.orchestrator.worker;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Durable worker lifecycle registry used by hosted submit admission.
 */
public interface PipelineWorkerRegistry {

    Uni<PipelineWorkerRecord> register(PipelineWorkerRegistration registration, long nowEpochMs);

    Uni<Optional<PipelineWorkerRecord>> heartbeat(
        String tenantId,
        String pipelineId,
        String workerId,
        long nowEpochMs,
        Duration staleAfter);

    Uni<Optional<PipelineWorkerRecord>> markDraining(
        String tenantId,
        String pipelineId,
        String workerId,
        long nowEpochMs,
        Duration staleAfter);

    Uni<List<PipelineWorkerRecord>> list(
        String tenantId,
        String pipelineId,
        long nowEpochMs,
        Duration staleAfter);

    default Uni<List<PipelineWorkerRecord>> matching(
        PipelineWorkerAvailabilityRequest request,
        String providerName,
        long nowEpochMs,
        Duration staleAfter) {
        return list(request.tenantId(), request.pipelineId(), nowEpochMs, staleAfter)
            .onItem().transform(records -> records.stream()
                .filter(record -> record.matches(request, providerName)
                    || record.hasArtifactMismatch(request, providerName))
                .toList());
    }
}
