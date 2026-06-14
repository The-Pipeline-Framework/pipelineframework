package org.pipelineframework.orchestrator.worker;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * In-memory worker lifecycle registry for tests and local demos.
 */
public class InMemoryPipelineWorkerRegistry implements PipelineWorkerRegistry {

    private final Object lock = new Object();
    private final Map<String, PipelineWorkerRecord> workers = new HashMap<>();

    @Override
    public Uni<PipelineWorkerRecord> register(PipelineWorkerRegistration registration, long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                PipelineWorkerRecord record = new PipelineWorkerRecord(
                    registration.tenantId(),
                    registration.pipelineId(),
                    registration.contractVersion(),
                    registration.releaseVersion(),
                    registration.workerId(),
                    registration.protocol(),
                    registration.endpoint(),
                    registration.artifactId(),
                    registration.artifactDigest(),
                    PipelineWorkerState.HEALTHY,
                    nowEpochMs,
                    nowEpochMs,
                    0L);
                workers.put(key(registration.tenantId(), registration.pipelineId(), registration.workerId()), record);
                return record;
            }
        });
    }

    @Override
    public Uni<Optional<PipelineWorkerRecord>> heartbeat(
        String tenantId,
        String pipelineId,
        String workerId,
        long nowEpochMs,
        Duration staleAfter) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                PipelineWorkerRecord current = workers.get(key(tenantId, pipelineId, workerId));
                if (current == null) {
                    return Optional.empty();
                }
                PipelineWorkerRecord updated = new PipelineWorkerRecord(
                    current.tenantId(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.workerId(),
                    current.protocol(),
                    current.endpoint(),
                    current.artifactId(),
                    current.artifactDigest(),
                    current.drainingSinceEpochMs() > 0L ? PipelineWorkerState.DRAINING : PipelineWorkerState.HEALTHY,
                    current.registeredAtEpochMs(),
                    nowEpochMs,
                    current.drainingSinceEpochMs());
                workers.put(key(tenantId, pipelineId, workerId), updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<PipelineWorkerRecord>> markDraining(
        String tenantId,
        String pipelineId,
        String workerId,
        long nowEpochMs,
        Duration staleAfter) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                PipelineWorkerRecord current = workers.get(key(tenantId, pipelineId, workerId));
                if (current == null) {
                    return Optional.empty();
                }
                PipelineWorkerRecord updated = new PipelineWorkerRecord(
                    current.tenantId(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.workerId(),
                    current.protocol(),
                    current.endpoint(),
                    current.artifactId(),
                    current.artifactDigest(),
                    PipelineWorkerState.DRAINING,
                    current.registeredAtEpochMs(),
                    current.lastHeartbeatAtEpochMs(),
                    nowEpochMs);
                workers.put(key(tenantId, pipelineId, workerId), updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<List<PipelineWorkerRecord>> list(
        String tenantId,
        String pipelineId,
        long nowEpochMs,
        Duration staleAfter) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return workers.values().stream()
                    .filter(record -> record.tenantId().equals(tenantId) && record.pipelineId().equals(pipelineId))
                    .map(record -> effectiveState(record, nowEpochMs, staleAfter))
                    .toList();
            }
        });
    }

    private static PipelineWorkerRecord effectiveState(
        PipelineWorkerRecord record,
        long nowEpochMs,
        Duration staleAfter) {
        if (record.drainingSinceEpochMs() > 0L) {
            return record;
        }
        long staleAfterMs = staleAfter == null ? Duration.ofMinutes(2).toMillis() : staleAfter.toMillis();
        PipelineWorkerState state = staleAfterMs > 0L && nowEpochMs - record.lastHeartbeatAtEpochMs() > staleAfterMs
            ? PipelineWorkerState.STALE
            : PipelineWorkerState.HEALTHY;
        return new PipelineWorkerRecord(
            record.tenantId(),
            record.pipelineId(),
            record.contractVersion(),
            record.releaseVersion(),
            record.workerId(),
            record.protocol(),
            record.endpoint(),
            record.artifactId(),
            record.artifactDigest(),
            state,
            record.registeredAtEpochMs(),
            record.lastHeartbeatAtEpochMs(),
            record.drainingSinceEpochMs());
    }

    private static String key(String tenantId, String pipelineId, String workerId) {
        return tenantId + "\u001f" + pipelineId + "\u001f" + workerId;
    }
}
