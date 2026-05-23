package org.pipelineframework.awaitable;

import java.util.Map;

/**
 * Durable projection for a single await interaction.
 *
 * @param tenantId tenant id
 * @param executionId owning execution id
 * @param stepId owning await step id
 * @param stepIndex owning await step index
 * @param outputType expected await output Java type
 * @param interactionId framework-owned interaction id
 * @param correlationId adapter-visible correlation id
 * @param causationId id that caused this interaction
 * @param idempotencyKey duplicate suppression key
 * @param version optimistic-concurrency version
 * @param status interaction status
 * @param requestPayload request snapshot
 * @param responsePayload response snapshot
 * @param barrierId internal barrier id for per-item await interactions
 * @param barrierItemIndex zero-based item index inside the barrier
 * @param barrierItemCount expected item count inside the barrier
 * @param actor completing actor, if any
 * @param assignee assigned user, if any
 * @param group assigned group, if any
 * @param transportType adapter type
 * @param transportMetadata transport metadata
 * @param deadlineEpochMs absolute deadline
 * @param createdAtEpochMs creation timestamp
 * @param updatedAtEpochMs update timestamp
 * @param ttlEpochS expiry timestamp
 */
public record AwaitInteractionRecord(
    String tenantId,
    String executionId,
    String stepId,
    int stepIndex,
    String outputType,
    String interactionId,
    String correlationId,
    String causationId,
    String idempotencyKey,
    long version,
    AwaitInteractionStatus status,
    Object requestPayload,
    Object responsePayload,
    String barrierId,
    Integer barrierItemIndex,
    Integer barrierItemCount,
    String actor,
    String assignee,
    String group,
    String transportType,
    Map<String, Object> transportMetadata,
    long deadlineEpochMs,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS
) {
    public AwaitInteractionRecord(
        String tenantId,
        String executionId,
        String stepId,
        int stepIndex,
        String outputType,
        String interactionId,
        String correlationId,
        String causationId,
        String idempotencyKey,
        long version,
        AwaitInteractionStatus status,
        Object requestPayload,
        Object responsePayload,
        String actor,
        String assignee,
        String group,
        String transportType,
        Map<String, Object> transportMetadata,
        long deadlineEpochMs,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        long ttlEpochS
    ) {
        this(
            tenantId,
            executionId,
            stepId,
            stepIndex,
            outputType,
            interactionId,
            correlationId,
            causationId,
            idempotencyKey,
            version,
            status,
            requestPayload,
            responsePayload,
            null,
            null,
            null,
            actor,
            assignee,
            group,
            transportType,
            transportMetadata,
            deadlineEpochMs,
            createdAtEpochMs,
            updatedAtEpochMs,
            ttlEpochS);
    }

    public AwaitInteractionRecord {
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
        if (interactionId == null || interactionId.isBlank()) {
            throw new IllegalArgumentException("interactionId must not be blank");
        }
        if (correlationId == null || correlationId.isBlank()) {
            throw new IllegalArgumentException("correlationId must not be blank");
        }
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (barrierId != null && barrierId.isBlank()) {
            throw new IllegalArgumentException("barrierId must not be blank when set");
        }
        if (barrierId == null && (barrierItemIndex != null || barrierItemCount != null)) {
            throw new IllegalArgumentException("barrier item metadata requires barrierId");
        }
        if (barrierId != null) {
            if (barrierItemIndex == null || barrierItemIndex < 0) {
                throw new IllegalArgumentException("barrierItemIndex must be non-negative when barrierId is set");
            }
            if (barrierItemCount == null || barrierItemCount <= 0) {
                throw new IllegalArgumentException("barrierItemCount must be positive when barrierId is set");
            }
            if (barrierItemIndex >= barrierItemCount) {
                throw new IllegalArgumentException("barrierItemIndex must be less than barrierItemCount");
            }
        }
        transportMetadata = transportMetadata == null ? Map.of() : Map.copyOf(transportMetadata);
    }

    public boolean barrierItem() {
        return barrierId != null;
    }
}
