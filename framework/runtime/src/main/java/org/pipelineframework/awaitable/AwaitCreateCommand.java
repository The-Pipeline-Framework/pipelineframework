package org.pipelineframework.awaitable;

import java.time.Instant;

/**
 * Command used to create or deduplicate an await interaction.
 *
 * @param tenantId tenant that owns the execution
 * @param executionId owning queue-async execution identifier
 * @param stepId authored await step identifier
 * @param stepIndex authored await step index
 * @param outputType expected completion payload type
 * @param causationId stable causation identifier for deduplication
 * @param idempotencyKey stable interaction idempotency key
 * @param correlationId external correlation identifier
 * @param requestPayload payload dispatched to the external actor
 * @param assignee optional human assignee
 * @param group optional human group
 * @param transportType await transport type
 * @param unitId durable await unit identifier
 * @param itemIndex zero-based item index for per-item await interactions
 * @param nowEpochMs command time in epoch milliseconds
 * @param deadlineEpochMs completion deadline in epoch milliseconds
 * @param ttlEpochS expiry time in epoch seconds
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
    String unitId,
    Integer itemIndex,
    long nowEpochMs,
    long deadlineEpochMs,
    long ttlEpochS
) {
    public AwaitCreateCommand(
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
        this(
            tenantId,
            executionId,
            stepId,
            stepIndex,
            outputType,
            causationId,
            idempotencyKey,
            correlationId,
            requestPayload,
            assignee,
            group,
            transportType,
            causationId,
            null,
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
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must not be negative");
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
        if (unitId == null || unitId.isBlank()) {
            throw new IllegalArgumentException("unitId must not be blank");
        }
        if (itemIndex != null && itemIndex < 0) {
            throw new IllegalArgumentException("itemIndex must be non-negative when set");
        }
        if (nowEpochMs <= 0) {
            nowEpochMs = System.currentTimeMillis();
        }
        if (deadlineEpochMs <= nowEpochMs) {
            throw new IllegalArgumentException("deadlineEpochMs must be after nowEpochMs");
        }
        if (ttlEpochS <= 0) {
            ttlEpochS = Instant.ofEpochMilli(deadlineEpochMs).plusSeconds(86_400).getEpochSecond();
        } else if (ttlEpochS < Instant.ofEpochMilli(deadlineEpochMs).getEpochSecond()) {
            throw new IllegalArgumentException("ttlEpochS must not be before deadlineEpochMs");
        }
    }
}
