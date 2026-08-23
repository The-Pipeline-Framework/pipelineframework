package org.pipelineframework.command;

import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Stores managed command effect state.
 *
 * Implementations own the durable command-id index and should encode persistence,
 * optimistic-lock, and connectivity failures as failed {@link Uni} items. Transition
 * methods are expected to fail on missing command ids or illegal state transitions;
 * callers do not perform multi-writer conflict resolution.
 *
 * Implementations must preserve the output type recorded by {@link #markSucceeded}
 * so replayed command results can be returned without lossy casts or schema drift.
 */
public interface CommandEffectStore {
    /**
     * Returns the current effect record for the tenant and command id, or empty when no
     * effect has been recorded yet.
     */
    Uni<Optional<CommandEffectRecord>> find(String tenantId, String commandId);

    /**
     * Creates the initial pending record for a command request. Implementations should
     * fail the returned {@link Uni} when the command id already exists.
     */
    Uni<CommandEffectRecord> createPending(CommandRequest<?> request, long nowEpochMs);

    /**
     * Whether this store can atomically append and persist deliberate Command attempts.
     * Implementations returning {@code true} must override {@code createRetryAttempt}
     * and every attempt-id-aware transition overload; the compatibility defaults discard
     * the attempt id and are only suitable for stores that retain the {@code false} default.
     */
    default boolean supportsRetryAttempts() {
        return false;
    }

    /**
     * Atomically appends the next attempt when, and only when, the logical effect is
     * currently {@link CommandEffectStatus#FAILED_RETRYABLE}.
     */
    default Uni<CommandEffectRecord> createRetryAttempt(CommandRequest<?> request, long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "deliberate Command retry requires a CommandEffectStore that persists attempt history"));
    }

    /**
     * Marks an existing pending command as dispatching. Retrying this transition may be
     * accepted only when the stored state already reflects the same dispatch.
     */
    Uni<CommandEffectRecord> markDispatching(String tenantId, String commandId, long nowEpochMs);

    default Uni<CommandEffectRecord> markDispatching(
        String tenantId,
        String commandId,
        String attemptId,
        long nowEpochMs
    ) {
        return markDispatching(tenantId, commandId, nowEpochMs);
    }

    /**
     * Records the successful command output. The stored output type must remain compatible
     * with the command step output type for duplicate replay.
     */
    Uni<CommandEffectRecord> markSucceeded(String tenantId, String commandId, Object output, long nowEpochMs);

    default Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        String attemptId,
        Object output,
        long nowEpochMs
    ) {
        return markSucceeded(tenantId, commandId, output, nowEpochMs);
    }

    /**
     * Whether this store can durably preserve native command outcome snapshots. Native command
     * dispatch checks this before it creates any effect state, retaining source compatibility for
     * legacy store implementations that only support legacy commands.
     */
    default boolean supportsNativeOutcomeSnapshots() {
        return false;
    }

    /**
     * Records a successful native command output and its sanitized outcome snapshot.
     * Implementations that preserve native outcome snapshots must override this method.
     */
    default Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        Object output,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "native command outcomes require a CommandEffectStore that persists outcome snapshots"));
    }

    default Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        String attemptId,
        Object output,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return markSucceeded(tenantId, commandId, output, outcome, nowEpochMs);
    }

    /**
     * Records a retryable command failure. Implementations should retain enough error
     * detail for operators to classify and retry the effect.
     */
    Uni<CommandEffectRecord> markFailed(String tenantId, String commandId, Throwable failure, long nowEpochMs);

    default Uni<CommandEffectRecord> markFailed(
        String tenantId,
        String commandId,
        String attemptId,
        Throwable failure,
        long nowEpochMs
    ) {
        return markFailed(tenantId, commandId, failure, nowEpochMs);
    }

    /**
     * Records a terminal command failure that should be routed to dead-letter handling.
     */
    Uni<CommandEffectRecord> markDlq(String tenantId, String commandId, Throwable failure, long nowEpochMs);

    default Uni<CommandEffectRecord> markDlq(
        String tenantId,
        String commandId,
        String attemptId,
        Throwable failure,
        long nowEpochMs
    ) {
        return markDlq(tenantId, commandId, failure, nowEpochMs);
    }

    /**
     * Records a non-success native command outcome with its durable terminal or retryable state.
     * Implementations that preserve native outcome snapshots must override this method.
     */
    default Uni<CommandEffectRecord> markOutcome(
        String tenantId,
        String commandId,
        CommandEffectStatus status,
        Throwable failure,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "native command outcomes require a CommandEffectStore that persists outcome snapshots"));
    }

    default Uni<CommandEffectRecord> markOutcome(
        String tenantId,
        String commandId,
        String attemptId,
        CommandEffectStatus status,
        Throwable failure,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return markOutcome(tenantId, commandId, status, failure, outcome, nowEpochMs);
    }
}
