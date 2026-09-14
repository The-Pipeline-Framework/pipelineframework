package org.pipelineframework.command;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable state snapshot for one actual dispatch attempt of a logical Command effect.
 */
public record CommandEffectAttemptRecord(
    String attemptId,
    String occurrenceId,
    int attemptNumber,
    String executionId,
    CommandAttemptPurpose purpose,
    CommandEffectStatus status,
    Optional<Object> output,
    String errorClass,
    String errorMessage,
    Optional<CommandOutcomeSnapshot> outcome,
    Optional<String> reason,
    long createdAtEpochMs,
    long updatedAtEpochMs
) {
    public CommandEffectAttemptRecord {
        if (attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("attemptId must not be blank");
        }
        if (occurrenceId == null || occurrenceId.isBlank()) {
            throw new IllegalArgumentException("occurrenceId must not be blank");
        }
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be positive");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(purpose, "purpose must not be null");
        output = output == null ? Optional.empty() : output;
        outcome = outcome == null ? Optional.empty() : outcome;
        reason = reason == null ? Optional.empty() : reason.map(String::trim).filter(value -> !value.isEmpty());
        if (purpose == CommandAttemptPurpose.REISSUE && reason.isEmpty()) {
            throw new IllegalArgumentException("reissue attempt reason must not be blank");
        }
        if (createdAtEpochMs < 0 || updatedAtEpochMs < createdAtEpochMs) {
            throw new IllegalArgumentException("invalid attempt timestamps");
        }
    }

    public CommandEffectAttemptRecord withStatus(CommandEffectStatus newStatus, long nowEpochMs) {
        return new CommandEffectAttemptRecord(
            attemptId, occurrenceId, attemptNumber, executionId, purpose, newStatus, output,
            errorClass, errorMessage, outcome, reason, createdAtEpochMs, nowEpochMs);
    }

    public CommandEffectAttemptRecord succeeded(
        Object commandOutput,
        CommandOutcomeSnapshot snapshot,
        long nowEpochMs
    ) {
        return terminal(
            CommandEffectStatus.SUCCEEDED,
            Optional.ofNullable(commandOutput),
            null,
            snapshot,
            nowEpochMs);
    }

    public CommandEffectAttemptRecord failed(
        CommandEffectStatus failureStatus,
        Throwable failure,
        CommandOutcomeSnapshot snapshot,
        long nowEpochMs
    ) {
        return terminal(failureStatus, Optional.empty(), failure, snapshot, nowEpochMs);
    }

    private CommandEffectAttemptRecord terminal(
        CommandEffectStatus terminalStatus,
        Optional<Object> terminalOutput,
        Throwable failure,
        CommandOutcomeSnapshot snapshot,
        long nowEpochMs
    ) {
        return new CommandEffectAttemptRecord(
            attemptId,
            occurrenceId,
            attemptNumber,
            executionId,
            purpose,
            terminalStatus,
            terminalOutput,
            failure == null ? null : failure.getClass().getName(),
            failure == null ? null : failure.getMessage(),
            Optional.ofNullable(snapshot),
            reason,
            createdAtEpochMs,
            nowEpochMs);
    }
}
