package org.pipelineframework.command;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;

/**
 * In-memory command effect store for tests, dev, and local examples.
 */
@ApplicationScoped
public class InMemoryCommandEffectStore implements CommandEffectStore {
    private final Map<String, CommandEffectRecord> records = new ConcurrentHashMap<>();

    @Override
    public Uni<Optional<CommandEffectRecord>> find(String tenantId, String commandId) {
        return Uni.createFrom().item(Optional.ofNullable(records.get(key(tenantId, commandId))));
    }

    @Override
    public Uni<CommandEffectRecord> createPending(CommandRequest<?> request, long nowEpochMs) {
        CommandEffectRecord pending = new CommandEffectRecord(
            request.executionContext().tenantId(),
            request.executionContext().executionId(),
            request.descriptor().stepId(),
            request.descriptor().command(),
            request.commandId(),
            CommandEffectStatus.PENDING,
            request.input(),
            null,
            null,
            null,
            nowEpochMs,
            nowEpochMs);
        CommandEffectRecord existing = records.putIfAbsent(key(pending.tenantId(), pending.commandId()), pending);
        if (existing != null) {
            throw new IllegalStateException("Command effect record already exists for commandId " + pending.commandId());
        }
        return Uni.createFrom().item(pending);
    }

    @Override
    public Uni<CommandEffectRecord> markDispatching(String tenantId, String commandId, long nowEpochMs) {
        return update(tenantId, commandId, record -> record.withStatus(CommandEffectStatus.DISPATCHING, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(String tenantId, String commandId, Object output, long nowEpochMs) {
        return update(tenantId, commandId, record -> record.succeeded(output, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markFailed(String tenantId, String commandId, Throwable failure, long nowEpochMs) {
        return update(tenantId, commandId, record -> record.failed(failure, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markDlq(String tenantId, String commandId, Throwable failure, long nowEpochMs) {
        return update(tenantId, commandId, record -> record.dlq(failure, nowEpochMs));
    }

    public void clear() {
        records.clear();
    }

    private Uni<CommandEffectRecord> update(
        String tenantId,
        String commandId,
        java.util.function.Function<CommandEffectRecord, CommandEffectRecord> updater
    ) {
        String key = key(tenantId, commandId);
        CommandEffectRecord updated = records.compute(key, (ignored, existing) -> {
            if (existing == null) {
                throw new IllegalStateException("No command effect record found for commandId " + commandId);
            }
            return updater.apply(existing);
        });
        return Uni.createFrom().item(updated);
    }

    private static String key(String tenantId, String commandId) {
        return tenantId + ":" + commandId;
    }
}
