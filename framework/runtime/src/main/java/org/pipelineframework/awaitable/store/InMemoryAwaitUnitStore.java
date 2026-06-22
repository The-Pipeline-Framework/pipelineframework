package org.pipelineframework.awaitable.store;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;

/**
 * In-memory await unit store for local development and tests.
 */
@ApplicationScoped
public class InMemoryAwaitUnitStore implements AwaitUnitStore {

    private final Object lock = new Object();
    private final Map<String, AwaitUnitRecord> unitsByScopedId = new HashMap<>();
    private final Map<String, Set<String>> completedItemsByScopedId = new HashMap<>();

    @Override
    public String providerName() {
        return "memory";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<AwaitUnitRecord> createOrGet(AwaitUnitCreateCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(command.nowEpochMs());
                String scopedId = scopedUnitId(command.tenantId(), command.unitId());
                AwaitUnitRecord existing = unitsByScopedId.get(scopedId);
                if (existing != null) {
                    return existing;
                }
                AwaitUnitRecord created = new AwaitUnitRecord(
                    command.tenantId(),
                    command.unitId(),
                    command.executionId(),
                    command.stepId(),
                    command.stepIndex(),
                    command.cardinality(),
                    0L,
                    AwaitUnitStatus.WAITING_EXTERNAL,
                    null,
                    null,
                    0,
                    java.util.Set.of(),
                    false,
                    command.nowEpochMs(),
                    command.nowEpochMs(),
                    command.ttlEpochS());
                unitsByScopedId.put(scopedId, created);
                return created;
            }
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> get(String tenantId, String unitId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(System.currentTimeMillis());
                return Optional.ofNullable(unitsByScopedId.get(scopedUnitId(tenantId, unitId)));
            }
        });
    }

    @Override
    public Uni<AwaitUnitRecord> importRecord(AwaitUnitRecord record) {
        return Uni.createFrom().item(() -> {
            if (record == null) {
                throw new IllegalArgumentException("Await unit record is required");
            }
            synchronized (lock) {
                purgeExpired(System.currentTimeMillis());
                String scopedId = scopedUnitId(record.tenantId(), record.unitId());
                AwaitUnitRecord existing = unitsByScopedId.get(scopedId);
                if (existing != null) {
                    return existing;
                }
                unitsByScopedId.put(scopedId, record);
                completedItemsByScopedId.put(scopedId, new HashSet<>(record.completedItemKeys()));
                return record;
            }
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> attachPrimaryInteraction(
        String tenantId,
        String unitId,
        String interactionId,
        long nowEpochMs) {
        return update(tenantId, unitId, nowEpochMs, current -> new AwaitUnitRecord(
            current.tenantId(),
            current.unitId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.cardinality(),
            current.version() + 1,
            current.status(),
            interactionId,
            current.expectedItemCount(),
            current.completedItemCount(),
            current.completedItemKeys(),
            current.dispatchComplete(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS()));
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markDispatchComplete(
        String tenantId,
        String unitId,
        int expectedItemCount,
        long nowEpochMs) {
        return update(tenantId, unitId, nowEpochMs, current -> {
            AwaitUnitStatus status = current.completedItemCount() == expectedItemCount
                ? AwaitUnitStatus.COMPLETED
                : current.status();
            return new AwaitUnitRecord(
                current.tenantId(),
                current.unitId(),
                current.executionId(),
                current.stepId(),
                current.stepIndex(),
                current.cardinality(),
                current.version() + 1,
                status,
                current.primaryInteractionId(),
                expectedItemCount,
                current.completedItemCount(),
                current.completedItemKeys(),
                true,
                current.createdAtEpochMs(),
                nowEpochMs,
                current.ttlEpochS());
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> recordItemCompleted(
        String tenantId,
        String unitId,
        String itemCompletionKey,
        long nowEpochMs) {
        if (itemCompletionKey == null || itemCompletionKey.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("itemCompletionKey must not be blank"));
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedUnitId(tenantId, unitId);
                AwaitUnitRecord current = unitsByScopedId.get(scopedId);
                if (current == null || current.status().terminal()) {
                    return Optional.ofNullable(current);
                }
                Set<String> completedItems = completedItemsByScopedId.computeIfAbsent(scopedId, ignored -> new HashSet<>());
                if (!completedItems.add(itemCompletionKey)) {
                    return Optional.of(current);
                }
                int completed = current.completedItemCount() + 1;
                boolean terminal = current.dispatchComplete()
                    && current.expectedItemCount() != null
                    && completed >= current.expectedItemCount();
                AwaitUnitRecord updated = new AwaitUnitRecord(
                    current.tenantId(),
                    current.unitId(),
                    current.executionId(),
                    current.stepId(),
                    current.stepIndex(),
                    current.cardinality(),
                    current.version() + 1,
                    terminal ? AwaitUnitStatus.COMPLETED : current.status(),
                    current.primaryInteractionId(),
                    current.expectedItemCount(),
                    completed,
                    java.util.Set.copyOf(completedItems),
                    current.dispatchComplete(),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS());
                unitsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markCompleted(String tenantId, String unitId, long nowEpochMs) {
        return update(tenantId, unitId, nowEpochMs, current -> new AwaitUnitRecord(
            current.tenantId(),
            current.unitId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.cardinality(),
            current.version() + 1,
            AwaitUnitStatus.COMPLETED,
            current.primaryInteractionId(),
            current.expectedItemCount(),
            current.completedItemCount(),
            current.completedItemKeys(),
            current.dispatchComplete(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS()));
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markTerminal(
        String tenantId,
        String unitId,
        AwaitUnitStatus status,
        long nowEpochMs) {
        return update(tenantId, unitId, nowEpochMs, current -> new AwaitUnitRecord(
            current.tenantId(),
            current.unitId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.cardinality(),
            current.version() + 1,
            status,
            current.primaryInteractionId(),
            current.expectedItemCount(),
            current.completedItemCount(),
            current.completedItemKeys(),
            current.dispatchComplete(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS()));
    }

    private Uni<Optional<AwaitUnitRecord>> update(
        String tenantId,
        String unitId,
        long nowEpochMs,
        java.util.function.Function<AwaitUnitRecord, AwaitUnitRecord> updater) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedUnitId(tenantId, unitId);
                AwaitUnitRecord current = unitsByScopedId.get(scopedId);
                if (current == null || current.status().terminal()) {
                    return Optional.ofNullable(current);
                }
                AwaitUnitRecord updated = updater.apply(current);
                unitsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    private void purgeExpired(long nowEpochMs) {
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        unitsByScopedId.entrySet().removeIf(entry -> {
            AwaitUnitRecord record = entry.getValue();
            boolean expired = record.ttlEpochS() > 0 && record.ttlEpochS() <= nowEpochS;
            if (expired) {
                completedItemsByScopedId.remove(entry.getKey());
            }
            return expired;
        });
    }

    private static String scopedUnitId(String tenantId, String unitId) {
        return tenantId + "::" + unitId;
    }
}
