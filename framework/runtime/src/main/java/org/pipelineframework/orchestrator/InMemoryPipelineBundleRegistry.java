package org.pipelineframework.orchestrator;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * In-memory hosted bundle registry for local/dev coordinator skeletons.
 */
public class InMemoryPipelineBundleRegistry implements PipelineBundleRegistry {

    private final Object lock = new Object();
    private final Map<String, PipelineBundleRecord> bundlesByKey = new HashMap<>();

    @Override
    public Uni<PipelineBundleRecord> register(PipelineBundleRecord record) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String key = key(record.tenantId(), record.pipelineId(), record.bundleVersionId());
                PipelineBundleRecord existing = bundlesByKey.get(key);
                if (existing != null) {
                    if (!existing.bundleHash().equals(record.bundleHash())
                        || !existing.artifactPath().equals(record.artifactPath())
                        || existing.artifactSizeBytes() != record.artifactSizeBytes()
                        || !existing.artifactChecksum().equals(record.artifactChecksum())) {
                        throw new IllegalStateException(
                            "Bundle version is already registered with different metadata");
                    }
                    return existing;
                }
                bundlesByKey.put(key, record);
                return record;
            }
        });
    }

    @Override
    public Uni<List<PipelineBundleRecord>> list(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return bundlesByKey.values().stream()
                    .filter(record -> record.tenantId().equals(tenantId) && record.pipelineId().equals(pipelineId))
                    .sorted(Comparator.comparingLong(PipelineBundleRecord::createdAtEpochMs))
                    .toList();
            }
        });
    }

    @Override
    public Uni<Optional<PipelineBundleRecord>> get(String tenantId, String pipelineId, String bundleVersionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return Optional.ofNullable(bundlesByKey.get(key(tenantId, pipelineId, bundleVersionId)));
            }
        });
    }

    @Override
    public Uni<Optional<PipelineBundleRecord>> active(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return bundlesByKey.values().stream()
                    .filter(record -> record.tenantId().equals(tenantId)
                        && record.pipelineId().equals(pipelineId)
                        && record.status() == PipelineBundleStatus.ACTIVE)
                    .findFirst();
            }
        });
    }

    @Override
    public Uni<Optional<PipelineBundleRecord>> activate(
        String tenantId,
        String pipelineId,
        String bundleVersionId,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String selectedKey = key(tenantId, pipelineId, bundleVersionId);
                PipelineBundleRecord selected = bundlesByKey.get(selectedKey);
                if (selected == null) {
                    return Optional.empty();
                }
                bundlesByKey.replaceAll((key, record) -> {
                    if (!record.tenantId().equals(tenantId) || !record.pipelineId().equals(pipelineId)) {
                        return record;
                    }
                    if (key.equals(selectedKey)) {
                        return record.withStatus(PipelineBundleStatus.ACTIVE, nowEpochMs);
                    }
                    if (record.status() == PipelineBundleStatus.ACTIVE) {
                        return record.withStatus(PipelineBundleStatus.REGISTERED, nowEpochMs);
                    }
                    return record;
                });
                return Optional.of(bundlesByKey.get(selectedKey));
            }
        });
    }

    private static String key(String tenantId, String pipelineId, String bundleVersionId) {
        return tenantId + "\u001f" + pipelineId + "\u001f" + bundleVersionId;
    }
}
