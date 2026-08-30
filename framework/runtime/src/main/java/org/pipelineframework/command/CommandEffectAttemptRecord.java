package org.pipelineframework.command;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable state snapshot for one actual dispatch attempt of a logical Command effect.
 */
public record CommandEffectAttemptRecord(
    String attemptId,
    int attemptNumber,
    String executionId,
    CommandEffectStatus status,
    String errorClass,
    String errorMessage,
    Optional<CommandOutcomeSnapshot> outcome,
    long createdAtEpochMs,
    long updatedAtEpochMs
) {
    public CommandEffectAttemptRecord {
        if (attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("attemptId must not be blank");
        }
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be positive");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        Objects.requireNonNull(status, "status must not be null");
        outcome = outcome == null ? Optional.empty() : outcome;
        if (createdAtEpochMs < 0 || updatedAtEpochMs < createdAtEpochMs) {
            throw new IllegalArgumentException("invalid attempt timestamps");
        }
    }

    public CommandEffectAttemptRecord withStatus(CommandEffectStatus newStatus, long nowEpochMs) {
        return new CommandEffectAttemptRecord(
            attemptId, attemptNumber, executionId, newStatus, errorClass, errorMessage,
            outcome, createdAtEpochMs, nowEpochMs);
    }

    public CommandEffectAttemptRecord succeeded(CommandOutcomeSnapshot snapshot, long nowEpochMs) {
        return terminal(CommandEffectStatus.SUCCEEDED, null, snapshot, nowEpochMs);
    }

    public CommandEffectAttemptRecord failed(
        CommandEffectStatus failureStatus,
        Throwable failure,
        CommandOutcomeSnapshot snapshot,
        long nowEpochMs
    ) {
        return terminal(failureStatus, failure, snapshot, nowEpochMs);
    }

    private CommandEffectAttemptRecord terminal(
        CommandEffectStatus terminalStatus,
        Throwable failure,
        CommandOutcomeSnapshot snapshot,
        long nowEpochMs
    ) {
        return new CommandEffectAttemptRecord(
            attemptId,
            attemptNumber,
            executionId,
            terminalStatus,
            failure == null ? null : failure.getClass().getName(),
            failure == null ? null : failure.getMessage(),
            Optional.ofNullable(snapshot),
            createdAtEpochMs,
            nowEpochMs);
    }
}
