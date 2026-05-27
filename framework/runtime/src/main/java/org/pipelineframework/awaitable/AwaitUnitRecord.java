package org.pipelineframework.awaitable;

/**
 * Durable orchestration record for one authored await unit.
 */
public record AwaitUnitRecord(
    String tenantId,
    String unitId,
    String executionId,
    String stepId,
    int stepIndex,
    String cardinality,
    long version,
    AwaitUnitStatus status,
    String primaryInteractionId,
    Integer expectedItemCount,
    int completedItemCount,
    boolean dispatchComplete,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS
) {
    public AwaitUnitRecord {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (unitId == null || unitId.isBlank()) {
            throw new IllegalArgumentException("unitId must not be blank");
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
        if (cardinality == null || cardinality.isBlank()) {
            throw new IllegalArgumentException("cardinality must not be blank");
        }
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (expectedItemCount != null && expectedItemCount < 0) {
            throw new IllegalArgumentException("expectedItemCount must not be negative");
        }
        if (completedItemCount < 0) {
            throw new IllegalArgumentException("completedItemCount must not be negative");
        }
        if (expectedItemCount != null && completedItemCount > expectedItemCount) {
            throw new IllegalArgumentException("completedItemCount must not exceed expectedItemCount");
        }
    }
}
