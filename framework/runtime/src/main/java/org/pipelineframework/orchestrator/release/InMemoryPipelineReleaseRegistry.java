package org.pipelineframework.orchestrator.release;

import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseStatus;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * In-memory release registry for local/dev coordinator skeletons.
 */
public class InMemoryPipelineReleaseRegistry implements PipelineReleaseRegistry {

    private final Object lock = new Object();
    private final Map<String, PipelineReleaseRecord> releasesByKey = new HashMap<>();

    @Override
    public Uni<PipelineReleaseRecord> register(PipelineReleaseRecord record) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String key = key(record.tenantId(), record.pipelineId(), record.releaseVersion());
                PipelineReleaseRecord existing = releasesByKey.get(key);
                if (existing != null) {
                    if (!sameMetadata(existing, record)) {
                        throw new IllegalStateException(
                            "Release version is already registered with different metadata");
                    }
                    return existing;
                }
                releasesByKey.put(key, record);
                return record;
            }
        });
    }

    @Override
    public Uni<List<PipelineReleaseRecord>> list(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return releasesByKey.values().stream()
                    .filter(record -> record.tenantId().equals(tenantId) && record.pipelineId().equals(pipelineId))
                    .sorted(Comparator.comparingLong(PipelineReleaseRecord::createdAtEpochMs))
                    .toList();
            }
        });
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> get(String tenantId, String pipelineId, String releaseVersion) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return Optional.ofNullable(releasesByKey.get(key(tenantId, pipelineId, releaseVersion)));
            }
        });
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> active(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return releasesByKey.values().stream()
                    .filter(record -> record.tenantId().equals(tenantId)
                        && record.pipelineId().equals(pipelineId)
                        && record.status() == PipelineReleaseStatus.ACTIVE)
                    .findFirst();
            }
        });
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> activate(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String selectedKey = key(tenantId, pipelineId, releaseVersion);
                if (!releasesByKey.containsKey(selectedKey)) {
                    return Optional.empty();
                }
                releasesByKey.replaceAll((key, record) -> {
                    if (!record.tenantId().equals(tenantId) || !record.pipelineId().equals(pipelineId)) {
                        return record;
                    }
                    if (key.equals(selectedKey)) {
                        return record.withStatus(PipelineReleaseStatus.ACTIVE, nowEpochMs);
                    }
                    if (record.status() == PipelineReleaseStatus.ACTIVE) {
                        return record.withStatus(PipelineReleaseStatus.REGISTERED, nowEpochMs);
                    }
                    return record;
                });
                return Optional.of(releasesByKey.get(selectedKey));
            }
        });
    }

    private static String key(String tenantId, String pipelineId, String releaseVersion) {
        return tenantId + "\u001f" + pipelineId + "\u001f" + releaseVersion;
    }

    private static boolean sameMetadata(PipelineReleaseRecord left, PipelineReleaseRecord right) {
        return left.contractVersion().equals(right.contractVersion())
            && left.primaryArtifactUri().equals(right.primaryArtifactUri())
            && left.primaryArtifactSizeBytes() == right.primaryArtifactSizeBytes()
            && left.primaryArtifactChecksum().equals(right.primaryArtifactChecksum());
    }
}
