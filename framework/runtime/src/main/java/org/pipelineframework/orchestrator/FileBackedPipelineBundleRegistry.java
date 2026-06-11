package org.pipelineframework.orchestrator;

import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * File-backed local/dev bundle registry metadata store.
 */
public class FileBackedPipelineBundleRegistry implements PipelineBundleRegistry {

    private static final TypeReference<List<PipelineBundleRecord>> RECORD_LIST_TYPE = new TypeReference<>() {
    };

    private final Object lock = new Object();
    private final Path registryPath;

    public FileBackedPipelineBundleRegistry(Path root) {
        if (root == null) {
            throw new IllegalArgumentException("Bundle registry root is required");
        }
        this.registryPath = root.toAbsolutePath().normalize().resolve("registry").resolve("bundles.json");
    }

    @Override
    public Uni<PipelineBundleRecord> register(PipelineBundleRecord record) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                List<PipelineBundleRecord> records = loadRecords();
                Optional<PipelineBundleRecord> existing = records.stream()
                    .filter(candidate -> keyMatches(candidate, record.tenantId(), record.pipelineId(), record.bundleVersionId()))
                    .findFirst();
                if (existing.isPresent()) {
                    PipelineBundleRecord value = existing.get();
                    if (!sameStoredMetadata(value, record)) {
                        throw new IllegalStateException(
                            "Bundle version is already registered with different metadata");
                    }
                    return value;
                }
                records.add(record);
                writeRecords(records);
                return record;
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<List<PipelineBundleRecord>> list(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return loadRecords().stream()
                    .filter(record -> record.tenantId().equals(tenantId) && record.pipelineId().equals(pipelineId))
                    .sorted(Comparator.comparingLong(PipelineBundleRecord::createdAtEpochMs))
                    .toList();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Optional<PipelineBundleRecord>> get(String tenantId, String pipelineId, String bundleVersionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return loadRecords().stream()
                    .filter(record -> keyMatches(record, tenantId, pipelineId, bundleVersionId))
                    .findFirst();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Optional<PipelineBundleRecord>> active(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return loadRecords().stream()
                    .filter(record -> record.tenantId().equals(tenantId)
                        && record.pipelineId().equals(pipelineId)
                        && record.status() == PipelineBundleStatus.ACTIVE)
                    .findFirst();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Optional<PipelineBundleRecord>> activate(
        String tenantId,
        String pipelineId,
        String bundleVersionId,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                List<PipelineBundleRecord> records = loadRecords();
                boolean found = records.stream()
                    .anyMatch(record -> keyMatches(record, tenantId, pipelineId, bundleVersionId));
                if (!found) {
                    return Optional.<PipelineBundleRecord>empty();
                }
                List<PipelineBundleRecord> updated = new ArrayList<>(records.size());
                PipelineBundleRecord selected = null;
                for (PipelineBundleRecord record : records) {
                    PipelineBundleRecord next = record;
                    if (record.tenantId().equals(tenantId) && record.pipelineId().equals(pipelineId)) {
                        if (record.bundleVersionId().equals(bundleVersionId)) {
                            next = record.withStatus(PipelineBundleStatus.ACTIVE, nowEpochMs);
                            selected = next;
                        } else if (record.status() == PipelineBundleStatus.ACTIVE) {
                            next = record.withStatus(PipelineBundleStatus.REGISTERED, nowEpochMs);
                        }
                    }
                    updated.add(next);
                }
                writeRecords(updated);
                return Optional.ofNullable(selected);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private List<PipelineBundleRecord> loadRecords() {
        if (!Files.isRegularFile(registryPath)) {
            return new ArrayList<>();
        }
        try {
            return new ArrayList<>(PipelineJson.mapper().readValue(registryPath.toFile(), RECORD_LIST_TYPE));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read bundle registry metadata: " + e.getMessage(), e);
        }
    }

    private void writeRecords(List<PipelineBundleRecord> records) {
        try {
            Files.createDirectories(registryPath.getParent());
            Path temp = Files.createTempFile(registryPath.getParent(), "bundles-", ".json");
            try {
                PipelineJson.mapper().writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), records);
                try {
                    Files.move(temp, registryPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(temp, registryPath, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(temp);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write bundle registry metadata: " + e.getMessage(), e);
        }
    }

    private static boolean keyMatches(
        PipelineBundleRecord record,
        String tenantId,
        String pipelineId,
        String bundleVersionId) {
        return record.tenantId().equals(tenantId)
            && record.pipelineId().equals(pipelineId)
            && record.bundleVersionId().equals(bundleVersionId);
    }

    private static boolean sameStoredMetadata(PipelineBundleRecord left, PipelineBundleRecord right) {
        return left.bundleHash().equals(right.bundleHash())
            && left.artifactPath().equals(right.artifactPath())
            && left.artifactSizeBytes() == right.artifactSizeBytes()
            && left.artifactChecksum().equals(right.artifactChecksum());
    }
}
