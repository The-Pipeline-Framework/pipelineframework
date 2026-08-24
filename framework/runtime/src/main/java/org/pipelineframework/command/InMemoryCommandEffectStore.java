package org.pipelineframework.command;

import java.util.List;
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
            null,
            List.of(new CommandEffectAttemptRecord(
                request.attemptId(),
                1,
                request.executionContext().executionId(),
                CommandEffectStatus.PENDING,
                null,
                null,
                Optional.empty(),
                nowEpochMs,
                nowEpochMs)),
            nowEpochMs,
            nowEpochMs);
        CommandEffectRecord existing = records.putIfAbsent(key(pending.tenantId(), pending.commandId()), pending);
        if (existing != null) {
            throw new IllegalStateException("Command effect record already exists for commandId " + pending.commandId());
        }
        return Uni.createFrom().item(pending);
    }

    @Override
    public boolean supportsRetryAttempts() {
        return true;
    }

    @Override
    public Uni<CommandEffectRecord> createRetryAttempt(CommandRequest<?> request, long nowEpochMs) {
        String key = key(request.executionContext().tenantId(), request.commandId());
        CommandEffectRecord updated = records.compute(key, (ignored, existing) -> {
            if (existing == null) {
                throw new IllegalStateException(
                    "No command effect record found for commandId " + request.commandId());
            }
            return existing.appendRetryAttempt(request, nowEpochMs);
        });
        return Uni.createFrom().item(updated);
    }

    @Override
    public Uni<CommandEffectRecord> markDispatching(String tenantId, String commandId, long nowEpochMs) {
        return update(
            tenantId, commandId,
            record -> record.dispatching(record.currentAttempt().attemptId(), nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markDispatching(
        String tenantId,
        String commandId,
        String attemptId,
        long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.dispatching(attemptId, nowEpochMs));
    }

    @Override
    public boolean supportsNativeOutcomeSnapshots() {
        return true;
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(String tenantId, String commandId, Object output, long nowEpochMs) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.succeeded(dispatching.currentAttempt().attemptId(), output, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        String attemptId,
        Object output,
        long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.succeeded(attemptId, output, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        Object output,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.succeeded(
                dispatching.currentAttempt().attemptId(), output, outcome, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        String attemptId,
        Object output,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.succeeded(attemptId, output, outcome, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markFailed(String tenantId, String commandId, Throwable failure, long nowEpochMs) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.failed(dispatching.currentAttempt().attemptId(), failure, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markFailed(
        String tenantId,
        String commandId,
        String attemptId,
        Throwable failure,
        long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.failed(attemptId, failure, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markDlq(String tenantId, String commandId, Throwable failure, long nowEpochMs) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.dlq(dispatching.currentAttempt().attemptId(), failure, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markDlq(
        String tenantId,
        String commandId,
        String attemptId,
        Throwable failure,
        long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.dlq(attemptId, failure, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markOutcome(
        String tenantId,
        String commandId,
        CommandEffectStatus status,
        Throwable failure,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.failedWithStatus(
                dispatching.currentAttempt().attemptId(), status, failure, outcome, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markOutcome(
        String tenantId,
        String commandId,
        String attemptId,
        CommandEffectStatus status,
        Throwable failure,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return update(
            tenantId, commandId,
            record -> record.failedWithStatus(attemptId, status, failure, outcome, nowEpochMs));
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

    private static CommandEffectRecord legacyCompletionSource(CommandEffectRecord record, long nowEpochMs) {
        return record.status() == CommandEffectStatus.PENDING
            ? record.dispatching(record.currentAttempt().attemptId(), nowEpochMs)
            : record;
    }
}
