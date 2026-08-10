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
 * @param unitId owning await unit id
 * @param itemIndex zero-based item index when the await unit owns multiple ordered items
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
    String unitId,
    Integer itemIndex,
    String actor,
    String assignee,
    String group,
    String transportType,
    Map<String, Object> transportMetadata,
    long deadlineEpochMs,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS,
    String transportOutputType,
    AwaitContinuationStatus continuationStatus,
    int continuationAttempt,
    long continuationNextDueEpochMs,
    String continuationLeaseOwner,
    long continuationLeaseExpiresEpochMs,
    Object continuationOutputPayload,
    String streamRegionId
) {
    /**
     * Compatibility constructor for rows written before item continuations were linked to a
     * producer-owned stream region.
     */
    public AwaitInteractionRecord(
        String tenantId, String executionId, String stepId, int stepIndex, String outputType,
        String interactionId, String correlationId, String causationId, String idempotencyKey, long version,
        AwaitInteractionStatus status, Object requestPayload, Object responsePayload, String unitId,
        Integer itemIndex, String actor, String assignee, String group, String transportType,
        Map<String, Object> transportMetadata, long deadlineEpochMs, long createdAtEpochMs,
        long updatedAtEpochMs, long ttlEpochS, String transportOutputType,
        AwaitContinuationStatus continuationStatus, int continuationAttempt, long continuationNextDueEpochMs,
        String continuationLeaseOwner, long continuationLeaseExpiresEpochMs, Object continuationOutputPayload
    ) {
        this(tenantId, executionId, stepId, stepIndex, outputType, interactionId, correlationId, causationId,
            idempotencyKey, version, status, requestPayload, responsePayload, unitId, itemIndex, actor, assignee,
            group, transportType, transportMetadata, deadlineEpochMs, createdAtEpochMs, updatedAtEpochMs,
            ttlEpochS, transportOutputType, continuationStatus, continuationAttempt, continuationNextDueEpochMs,
            continuationLeaseOwner, continuationLeaseExpiresEpochMs, continuationOutputPayload, "");
    }
    /** Compatibility constructor for interaction rows created before durable continuation work. */
    public AwaitInteractionRecord(
        String tenantId, String executionId, String stepId, int stepIndex, String outputType,
        String interactionId, String correlationId, String causationId, String idempotencyKey, long version,
        AwaitInteractionStatus status, Object requestPayload, Object responsePayload, String unitId,
        Integer itemIndex, String actor, String assignee, String group, String transportType,
        Map<String, Object> transportMetadata, long deadlineEpochMs, long createdAtEpochMs,
        long updatedAtEpochMs, long ttlEpochS, String transportOutputType
    ) {
        this(tenantId, executionId, stepId, stepIndex, outputType, interactionId, correlationId, causationId,
            idempotencyKey, version, status, requestPayload, responsePayload, unitId, itemIndex, actor, assignee,
            group, transportType, transportMetadata, deadlineEpochMs, createdAtEpochMs, updatedAtEpochMs,
            ttlEpochS, transportOutputType,
            itemIndex == null ? AwaitContinuationStatus.HELD : AwaitContinuationStatus.HELD,
            0, 0L, "", 0L, null);
    }
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
        String unitId,
        Integer itemIndex,
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
        this(tenantId, executionId, stepId, stepIndex, outputType, interactionId, correlationId, causationId,
            idempotencyKey, version, status, requestPayload, responsePayload, unitId, itemIndex, actor, assignee,
            group, transportType, transportMetadata, deadlineEpochMs, createdAtEpochMs, updatedAtEpochMs,
            ttlEpochS, outputType);
    }
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
            interactionId,
            null,
            actor,
            assignee,
            group,
            transportType,
            transportMetadata,
            deadlineEpochMs,
            createdAtEpochMs,
            updatedAtEpochMs,
            ttlEpochS,
            outputType);
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
        if (unitId == null || unitId.isBlank()) {
            throw new IllegalArgumentException("unitId must not be blank");
        }
        if (itemIndex != null && itemIndex < 0) {
            throw new IllegalArgumentException("itemIndex must be non-negative when set");
        }
        transportMetadata = transportMetadata == null ? Map.of() : Map.copyOf(transportMetadata);
        transportOutputType = transportOutputType == null || transportOutputType.isBlank()
            ? outputType
            : transportOutputType;
        continuationStatus = continuationStatus == null ? AwaitContinuationStatus.HELD : continuationStatus;
        if (continuationAttempt < 0) {
            throw new IllegalArgumentException("continuationAttempt must not be negative");
        }
        continuationLeaseOwner = continuationLeaseOwner == null ? "" : continuationLeaseOwner;
        streamRegionId = streamRegionId == null ? "" : streamRegionId;
    }

    public boolean itemInteraction() {
        return itemIndex != null;
    }

    /**
     * Returns a copy linked to the bounded producer region that created this item. The linkage is
     * immutable once assigned: an item continuation may return credit only to its own producer.
     */
    public AwaitInteractionRecord linkedToStreamRegion(String regionId) {
        if (regionId == null || regionId.isBlank()) {
            throw new IllegalArgumentException("stream region id must not be blank");
        }
        if (!streamRegionId.isBlank() && !streamRegionId.equals(regionId)) {
            throw new IllegalStateException("Await interaction is already linked to another stream region");
        }
        if (streamRegionId.equals(regionId)) {
            return this;
        }
        return new AwaitInteractionRecord(
            tenantId, executionId, stepId, stepIndex, outputType, interactionId, correlationId, causationId,
            idempotencyKey, version, status, requestPayload, responsePayload, unitId, itemIndex, actor, assignee,
            group, transportType, transportMetadata, deadlineEpochMs, createdAtEpochMs, updatedAtEpochMs,
            ttlEpochS, transportOutputType, continuationStatus, continuationAttempt, continuationNextDueEpochMs,
            continuationLeaseOwner, continuationLeaseExpiresEpochMs, continuationOutputPayload, regionId);
    }

    /**
     * Returns a transport-safe snapshot for a portable transition envelope.
     */
    public AwaitInteractionRecord withPayloadSnapshots(Object requestSnapshot, Object responseSnapshot) {
        return new AwaitInteractionRecord(
            tenantId, executionId, stepId, stepIndex, outputType, interactionId, correlationId, causationId,
            idempotencyKey, version, status, requestSnapshot, responseSnapshot, unitId, itemIndex, actor, assignee,
            group, transportType, transportMetadata, deadlineEpochMs, createdAtEpochMs, updatedAtEpochMs,
            ttlEpochS, transportOutputType, continuationStatus, continuationAttempt,
            continuationNextDueEpochMs, continuationLeaseOwner, continuationLeaseExpiresEpochMs,
            continuationOutputPayload, streamRegionId);
    }

    public boolean continuationEligible() {
        return itemInteraction() && status == AwaitInteractionStatus.COMPLETED
            && !continuationStatus.terminal();
    }

    public boolean continuationDue(long nowEpochMs) {
        if (!continuationEligible()) {
            return false;
        }
        if (continuationStatus == AwaitContinuationStatus.CLAIMED) {
            return continuationLeaseExpiresEpochMs <= nowEpochMs;
        }
        return continuationStatus.due(nowEpochMs)
            && continuationNextDueEpochMs <= nowEpochMs
            && (continuationLeaseOwner.isBlank() || continuationLeaseExpiresEpochMs <= nowEpochMs);
    }
}
