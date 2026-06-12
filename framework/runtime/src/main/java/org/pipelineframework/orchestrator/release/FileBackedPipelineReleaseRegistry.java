package org.pipelineframework.orchestrator.release;

import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseStatus;
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
 * File-backed local/dev release registry metadata store.
 */
public class FileBackedPipelineReleaseRegistry implements PipelineReleaseRegistry {

    private static final TypeReference<List<PipelineReleaseRecord>> RECORD_LIST_TYPE = new TypeReference<>() {
    };

    private final Object lock = new Object();
    private final Path registryPath;

    public FileBackedPipelineReleaseRegistry(Path root) {
        if (root == null) {
            throw new IllegalArgumentException("Release registry root is required");
        }
        this.registryPath = root.toAbsolutePath().normalize().resolve("registry").resolve("releases.json");
    }

    @Override
    public Uni<PipelineReleaseRecord> register(PipelineReleaseRecord record) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                List<PipelineReleaseRecord> records = loadRecords();
                Optional<PipelineReleaseRecord> existing = records.stream()
                    .filter(candidate -> keyMatches(candidate, record.tenantId(), record.pipelineId(), record.releaseVersion()))
                    .findFirst();
                if (existing.isPresent()) {
                    PipelineReleaseRecord value = existing.get();
                    if (!PipelineReleaseRecordMetadata.sameImmutableMetadata(value, record)) {
                        throw new IllegalStateException(
                            "Release version is already registered with different metadata");
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
    public Uni<List<PipelineReleaseRecord>> list(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return loadRecords().stream()
                    .filter(record -> record.tenantId().equals(tenantId) && record.pipelineId().equals(pipelineId))
                    .sorted(Comparator.comparingLong(PipelineReleaseRecord::createdAtEpochMs))
                    .toList();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> get(String tenantId, String pipelineId, String releaseVersion) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return loadRecords().stream()
                    .filter(record -> keyMatches(record, tenantId, pipelineId, releaseVersion))
                    .findFirst();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> active(String tenantId, String pipelineId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return loadRecords().stream()
                    .filter(record -> record.tenantId().equals(tenantId)
                        && record.pipelineId().equals(pipelineId)
                        && record.status() == PipelineReleaseStatus.ACTIVE)
                    .findFirst();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> activate(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                List<PipelineReleaseRecord> records = loadRecords();
                boolean found = records.stream()
                    .anyMatch(record -> keyMatches(record, tenantId, pipelineId, releaseVersion));
                if (!found) {
                    return Optional.<PipelineReleaseRecord>empty();
                }
                List<PipelineReleaseRecord> updated = new ArrayList<>(records.size());
                PipelineReleaseRecord selected = null;
                for (PipelineReleaseRecord record : records) {
                    PipelineReleaseRecord next = record;
                    if (record.tenantId().equals(tenantId) && record.pipelineId().equals(pipelineId)) {
                        if (record.releaseVersion().equals(releaseVersion)) {
                            next = record.withStatus(PipelineReleaseStatus.ACTIVE, nowEpochMs);
                            selected = next;
                        } else if (record.status() == PipelineReleaseStatus.ACTIVE) {
                            next = record.withStatus(PipelineReleaseStatus.REGISTERED, nowEpochMs);
                        }
                    }
                    updated.add(next);
                }
                writeRecords(updated);
                return Optional.ofNullable(selected);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private List<PipelineReleaseRecord> loadRecords() {
        if (!Files.isRegularFile(registryPath)) {
            return new ArrayList<>();
        }
        try {
            return new ArrayList<>(PipelineJson.mapper().readValue(registryPath.toFile(), RECORD_LIST_TYPE));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read release registry metadata: " + e.getMessage(), e);
        }
    }

    private void writeRecords(List<PipelineReleaseRecord> records) {
        try {
            Files.createDirectories(registryPath.getParent());
            Path temp = Files.createTempFile(registryPath.getParent(), "releases-", ".json");
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
            throw new IllegalStateException("Failed to write release registry metadata: " + e.getMessage(), e);
        }
    }

    private static boolean keyMatches(
        PipelineReleaseRecord record,
        String tenantId,
        String pipelineId,
        String releaseVersion) {
        return record.tenantId().equals(tenantId)
            && record.pipelineId().equals(pipelineId)
            && record.releaseVersion().equals(releaseVersion);
    }

}
