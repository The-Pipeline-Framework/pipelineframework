package org.pipelineframework.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Recorded state of one logical Command effect and its immutable dispatch-attempt history.
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
    List<CommandEffectAttemptRecord> attempts,
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
            tenantId, executionId, stepId, command, commandId, status, input, output,
            errorClass, errorMessage, Optional.empty(), List.of(), createdAtEpochMs, updatedAtEpochMs);
    }

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
        Optional<CommandOutcomeSnapshot> outcome,
        long createdAtEpochMs,
        long updatedAtEpochMs
    ) {
        this(
            tenantId, executionId, stepId, command, commandId, status, input, output,
            errorClass, errorMessage, outcome, List.of(), createdAtEpochMs, updatedAtEpochMs);
    }

    public CommandEffectRecord {
        outcome = outcome == null ? Optional.empty() : outcome;
        requireText(tenantId, "tenantId");
        requireText(executionId, "executionId");
        requireText(stepId, "stepId");
        requireText(command, "command");
        requireText(commandId, "commandId");
        Objects.requireNonNull(status, "status must not be null");
        if (createdAtEpochMs < 0) {
            throw new IllegalArgumentException("createdAtEpochMs must not be negative");
        }
        if (updatedAtEpochMs < createdAtEpochMs) {
            throw new IllegalArgumentException("updatedAtEpochMs must not be before createdAtEpochMs");
        }
        attempts = attempts == null || attempts.isEmpty()
            ? List.of(legacyAttempt(
                commandId, executionId, status, errorClass, errorMessage, outcome,
                createdAtEpochMs, updatedAtEpochMs))
            : List.copyOf(attempts);
        validateAttempts(attempts, status);
    }

    public CommandEffectAttemptRecord currentAttempt() {
        return attempts.getLast();
    }

    public CommandEffectRecord appendRetryAttempt(CommandRequest<?> request, long nowEpochMs) {
        Objects.requireNonNull(request, "request must not be null");
        if (status != CommandEffectStatus.FAILED_RETRYABLE) {
            throw new IllegalStateException(
                "Command effect " + commandId + " is not retryable from state " + status);
        }
        if (!commandId.equals(request.commandId())
            || !stepId.equals(request.descriptor().stepId())
            || !command.equals(request.descriptor().command())) {
            throw new IllegalArgumentException(
                "Retry request does not match the recorded logical command effect " + commandId);
        }
        List<CommandEffectAttemptRecord> updatedAttempts = new ArrayList<>(attempts);
        updatedAttempts.add(new CommandEffectAttemptRecord(
            request.attemptId(),
            currentAttempt().attemptNumber() + 1,
            request.executionContext().executionId(),
            CommandEffectStatus.PENDING,
            null,
            null,
            Optional.empty(),
            nowEpochMs,
            nowEpochMs));
        return copy(
            CommandEffectStatus.PENDING, null, null, null, Optional.empty(), updatedAttempts, nowEpochMs);
    }

    public CommandEffectRecord dispatching(String attemptId, long nowEpochMs) {
        requireCurrentAttempt(attemptId, CommandEffectStatus.PENDING);
        return copyWithCurrentAttempt(
            currentAttempt().withStatus(CommandEffectStatus.DISPATCHING, nowEpochMs),
            CommandEffectStatus.DISPATCHING, null, null, null, Optional.empty(), nowEpochMs);
    }

    public CommandEffectRecord succeeded(String attemptId, Object commandOutput, long nowEpochMs) {
        requireCurrentAttempt(attemptId, CommandEffectStatus.DISPATCHING);
        return copyWithCurrentAttempt(
            currentAttempt().succeeded(null, nowEpochMs),
            CommandEffectStatus.SUCCEEDED, commandOutput, null, null, Optional.empty(), nowEpochMs);
    }

    public CommandEffectRecord succeeded(
        String attemptId,
        Object commandOutput,
        CommandOutcomeSnapshot snapshot,
        long nowEpochMs
    ) {
        requireCurrentAttempt(attemptId, CommandEffectStatus.DISPATCHING);
        CommandOutcomeSnapshot required = Objects.requireNonNull(snapshot, "command outcome snapshot must not be null");
        return copyWithCurrentAttempt(
            currentAttempt().succeeded(required, nowEpochMs),
            CommandEffectStatus.SUCCEEDED, commandOutput, null, null, Optional.of(required), nowEpochMs);
    }

    public CommandEffectRecord failed(String attemptId, Throwable failure, long nowEpochMs) {
        return failedWithStatus(attemptId, CommandEffectStatus.FAILED_RETRYABLE, failure, null, nowEpochMs);
    }

    public CommandEffectRecord dlq(String attemptId, Throwable failure, long nowEpochMs) {
        return failedWithStatus(attemptId, CommandEffectStatus.DLQ, failure, null, nowEpochMs);
    }

    public CommandEffectRecord failedWithStatus(
        String attemptId,
        CommandEffectStatus failureStatus,
        Throwable failure,
        CommandOutcomeSnapshot snapshot,
        long nowEpochMs
    ) {
        requireCurrentAttempt(attemptId, CommandEffectStatus.DISPATCHING);
        String failureClass = failure == null ? null : failure.getClass().getName();
        String failureMessage = failure == null ? null : failure.getMessage();
        Optional<CommandOutcomeSnapshot> recordedOutcome = Optional.ofNullable(snapshot);
        return copyWithCurrentAttempt(
            currentAttempt().failed(failureStatus, failure, snapshot, nowEpochMs),
            failureStatus, output, failureClass, failureMessage, recordedOutcome, nowEpochMs);
    }

    private CommandEffectRecord copyWithCurrentAttempt(
        CommandEffectAttemptRecord updatedAttempt,
        CommandEffectStatus newStatus,
        Object newOutput,
        String newErrorClass,
        String newErrorMessage,
        Optional<CommandOutcomeSnapshot> newOutcome,
        long nowEpochMs
    ) {
        List<CommandEffectAttemptRecord> updatedAttempts = new ArrayList<>(attempts);
        updatedAttempts.set(updatedAttempts.size() - 1, updatedAttempt);
        return copy(
            newStatus, newOutput, newErrorClass, newErrorMessage,
            newOutcome, updatedAttempts, nowEpochMs);
    }

    private CommandEffectRecord copy(
        CommandEffectStatus newStatus,
        Object newOutput,
        String newErrorClass,
        String newErrorMessage,
        Optional<CommandOutcomeSnapshot> newOutcome,
        List<CommandEffectAttemptRecord> newAttempts,
        long nowEpochMs
    ) {
        return new CommandEffectRecord(
            tenantId, executionId, stepId, command, commandId, newStatus, input, newOutput,
            newErrorClass, newErrorMessage, newOutcome, newAttempts, createdAtEpochMs, nowEpochMs);
    }

    private void requireCurrentAttempt(String attemptId, CommandEffectStatus expectedStatus) {
        CommandEffectAttemptRecord current = currentAttempt();
        if (!current.attemptId().equals(attemptId)) {
            throw new IllegalStateException(
                "Attempt " + attemptId + " is not active for commandId " + commandId);
        }
        if (current.status() != expectedStatus) {
            throw new IllegalStateException(
                "Attempt " + attemptId + " is " + current.status() + ", expected " + expectedStatus);
        }
    }

    private static void validateAttempts(
        List<CommandEffectAttemptRecord> attempts,
        CommandEffectStatus currentStatus
    ) {
        int expectedNumber = 1;
        java.util.Set<String> ids = new java.util.HashSet<>();
        for (CommandEffectAttemptRecord attempt : attempts) {
            if (attempt.attemptNumber() != expectedNumber++) {
                throw new IllegalArgumentException("command attempt numbers must be contiguous from one");
            }
            if (!ids.add(attempt.attemptId())) {
                throw new IllegalArgumentException("command attempt IDs must be unique");
            }
        }
        if (attempts.getLast().status() != currentStatus) {
            throw new IllegalArgumentException("current effect status must match the latest attempt status");
        }
    }

    private static CommandEffectAttemptRecord legacyAttempt(
        String commandId,
        String executionId,
        CommandEffectStatus status,
        String errorClass,
        String errorMessage,
        Optional<CommandOutcomeSnapshot> outcome,
        long createdAtEpochMs,
        long updatedAtEpochMs
    ) {
        return new CommandEffectAttemptRecord(
            "legacy-" + Integer.toUnsignedString(commandId.hashCode(), 36),
            1,
            executionId,
            status,
            errorClass,
            errorMessage,
            outcome,
            createdAtEpochMs,
            updatedAtEpochMs);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
