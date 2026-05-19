package org.pipelineframework.awaitable;

import java.time.Instant;

/**
 * Command used to create or deduplicate an await interaction.
 */
public record AwaitCreateCommand(
    String tenantId,
    String executionId,
    String stepId,
    int stepIndex,
    String outputType,
    String causationId,
    String idempotencyKey,
    String correlationId,
    Object requestPayload,
    String assignee,
    String group,
    String transportType,
    long nowEpochMs,
    long deadlineEpochMs,
    long ttlEpochS
) {
    /**
     * Backward-compatible constructor for tests/callers created before step index and output type were persisted.
     */
    public AwaitCreateCommand(
        String tenantId,
        String executionId,
        String stepId,
        String causationId,
        String idempotencyKey,
        String correlationId,
        Object requestPayload,
        String assignee,
        String group,
        String transportType,
        long nowEpochMs,
        long deadlineEpochMs,
        long ttlEpochS
    ) {
        this(
            tenantId,
            executionId,
            stepId,
            0,
            Object.class.getName(),
            causationId,
            idempotencyKey,
            correlationId,
            requestPayload,
            assignee,
            group,
            transportType,
            nowEpochMs,
            deadlineEpochMs,
            ttlEpochS);
    }

    public AwaitCreateCommand {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("stepId must not be blank");
        }
        if (outputType == null || outputType.isBlank()) {
            throw new IllegalArgumentException("outputType must not be blank");
        }
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        if (correlationId == null || correlationId.isBlank()) {
            throw new IllegalArgumentException("correlationId must not be blank");
        }
        if (transportType == null || transportType.isBlank()) {
            throw new IllegalArgumentException("transportType must not be blank");
        }
        if (nowEpochMs <= 0) {
            nowEpochMs = System.currentTimeMillis();
        }
        if (deadlineEpochMs <= nowEpochMs) {
            throw new IllegalArgumentException("deadlineEpochMs must be after nowEpochMs");
        }
        if (ttlEpochS <= 0) {
            ttlEpochS = Instant.ofEpochMilli(deadlineEpochMs).plusSeconds(86_400).getEpochSecond();
        }
    }
}
