package org.pipelineframework.command;

import java.util.Objects;
import java.util.Optional;

/**
 * Recorded command effect state.
 */
public record CommandEffectRecord(
    String tenantId,
    String executionId,
    String stepId,
    String command,
    String commandId,
    CommandEffectStatus status,
    Object input,
    Object output,
    String errorClass,
    String errorMessage,
    Optional<CommandOutcomeSnapshot> outcome,
    long createdAtEpochMs,
    long updatedAtEpochMs
) {
    public CommandEffectRecord(
        String tenantId,
        String executionId,
        String stepId,
        String command,
        String commandId,
        CommandEffectStatus status,
        Object input,
        Object output,
        String errorClass,
        String errorMessage,
        long createdAtEpochMs,
        long updatedAtEpochMs
    ) {
        this(
            tenantId,
            executionId,
            stepId,
            command,
            commandId,
            status,
            input,
            output,
            errorClass,
            errorMessage,
            Optional.empty(),
            createdAtEpochMs,
            updatedAtEpochMs);
    }

    public CommandEffectRecord {
        outcome = outcome == null ? Optional.empty() : outcome;
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("stepId must not be blank");
        }
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("command must not be blank");
        }
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId must not be blank");
        }
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (createdAtEpochMs < 0) {
            throw new IllegalArgumentException("createdAtEpochMs must not be negative");
        }
        if (updatedAtEpochMs < 0) {
            throw new IllegalArgumentException("updatedAtEpochMs must not be negative");
        }
        if (updatedAtEpochMs < createdAtEpochMs) {
            throw new IllegalArgumentException("updatedAtEpochMs must not be before createdAtEpochMs");
        }
    }

    public CommandEffectRecord withStatus(CommandEffectStatus newStatus, long nowEpochMs) {
        return new CommandEffectRecord(
            tenantId,
            executionId,
            stepId,
            command,
            commandId,
            newStatus,
            input,
            output,
            errorClass,
            errorMessage,
            outcome,
            createdAtEpochMs,
            nowEpochMs);
    }

    public CommandEffectRecord succeeded(Object commandOutput, long nowEpochMs) {
        return new CommandEffectRecord(
            tenantId,
            executionId,
            stepId,
            command,
            commandId,
            CommandEffectStatus.SUCCEEDED,
            input,
            commandOutput,
            null,
            null,
            outcome,
            createdAtEpochMs,
            nowEpochMs);
    }

    public CommandEffectRecord succeeded(Object commandOutput, CommandOutcomeSnapshot snapshot, long nowEpochMs) {
        return new CommandEffectRecord(
            tenantId,
            executionId,
            stepId,
            command,
            commandId,
            CommandEffectStatus.SUCCEEDED,
            input,
            commandOutput,
            null,
            null,
            Optional.of(Objects.requireNonNull(snapshot, "command outcome snapshot must not be null")),
            createdAtEpochMs,
            nowEpochMs);
    }

    public CommandEffectRecord failed(Throwable failure, long nowEpochMs) {
        return failedWithStatus(CommandEffectStatus.FAILED_RETRYABLE, failure, nowEpochMs);
    }

    public CommandEffectRecord dlq(Throwable failure, long nowEpochMs) {
        return failedWithStatus(CommandEffectStatus.DLQ, failure, nowEpochMs);
    }

    private CommandEffectRecord failedWithStatus(CommandEffectStatus failureStatus, Throwable failure, long nowEpochMs) {
        String failureClass = failure == null ? null : failure.getClass().getName();
        String failureMessage = failure == null ? null : failure.getMessage();
        return new CommandEffectRecord(
            tenantId,
            executionId,
            stepId,
            command,
            commandId,
            failureStatus,
            input,
            output,
            failureClass,
            failureMessage,
            outcome,
            createdAtEpochMs,
            nowEpochMs);
    }

    public CommandEffectRecord failedWithStatus(
        CommandEffectStatus failureStatus,
        Throwable failure,
        CommandOutcomeSnapshot snapshot,
        long nowEpochMs
    ) {
        String failureClass = failure == null ? null : failure.getClass().getName();
        String failureMessage = failure == null ? null : failure.getMessage();
        return new CommandEffectRecord(
            tenantId,
            executionId,
            stepId,
            command,
            commandId,
            failureStatus,
            input,
            output,
            failureClass,
            failureMessage,
            Optional.of(Objects.requireNonNull(snapshot, "command outcome snapshot must not be null")),
            createdAtEpochMs,
            nowEpochMs);
    }
}
