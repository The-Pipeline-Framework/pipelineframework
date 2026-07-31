package org.pipelineframework.awaitable;

import java.util.Set;

/**
 * Durable orchestration record for one authored await unit.
 *
 * @param tenantId tenant that owns the execution
 * @param unitId durable await unit identifier
 * @param executionId owning queue-async execution identifier
 * @param stepId authored await step identifier
 * @param stepIndex authored await step index
 * @param cardinality authored await cardinality
 * @param version optimistic concurrency version
 * @param status current await unit status
 * @param primaryInteractionId primary externally visible interaction identifier
 * @param expectedItemCount expected item count for multi-item units, when known
 * @param completedItemCount admitted completed item count
 * @param completedItemKeys idempotency keys for admitted item completions and durable item-continuation facts
 * @param dispatchComplete whether dispatch has finished for the unit
 * @param createdAtEpochMs creation time in epoch milliseconds
 * @param updatedAtEpochMs last update time in epoch milliseconds
 * @param ttlEpochS expiry time in epoch seconds
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
    Set<String> completedItemKeys,
    boolean dispatchComplete,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS
) {
    public static final String CONTINUATION_COMPLETION_KEY_PREFIX = "continuation:";

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
        completedItemKeys = completedItemKeys == null ? Set.of() : Set.copyOf(completedItemKeys);
        long admittedCompletionKeyCount = completedItemKeys.stream()
            .filter(key -> !key.startsWith(CONTINUATION_COMPLETION_KEY_PREFIX))
            .count();
        // Historical await-unit rows did not persist per-item completion keys. Keep
        // those rows readable while validating every new row that carries keys.
        if (!completedItemKeys.isEmpty() && admittedCompletionKeyCount != completedItemCount) {
            throw new IllegalArgumentException("completedItemKeys must contain one non-continuation key per admitted item completion");
        }
    }

    public int completedContinuationItemCount() {
        return (int) completedItemKeys.stream()
            .filter(key -> key.startsWith(CONTINUATION_COMPLETION_KEY_PREFIX))
            .count();
    }

    public boolean hasContinuationCompletionFacts() {
        return completedContinuationItemCount() > 0;
    }

    public static String continuationCompletionKey(int itemIndex) {
        if (itemIndex < 0) {
            throw new IllegalArgumentException("itemIndex must not be negative");
        }
        return CONTINUATION_COMPLETION_KEY_PREFIX + itemIndex;
    }
}
