package org.pipelineframework.awaitable;

/**
 * Command used to create or deduplicate an await unit.
 */
public record AwaitUnitCreateCommand(
    String tenantId,
    String unitId,
    String executionId,
    String stepId,
    int stepIndex,
    String cardinality,
    long nowEpochMs,
    long ttlEpochS
) {
    public AwaitUnitCreateCommand {
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
        String normalizedCardinality = cardinality.trim().toUpperCase(java.util.Locale.ROOT);
        if (!normalizedCardinality.equals("ONE_TO_ONE")
            && !normalizedCardinality.equals("ONE_TO_MANY")
            && !normalizedCardinality.equals("MANY_TO_ONE")
            && !normalizedCardinality.equals("MANY_TO_MANY")) {
            throw new IllegalArgumentException("cardinality must be one of: ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY");
        }
        if (nowEpochMs <= 0) {
            nowEpochMs = System.currentTimeMillis();
        }
        if (ttlEpochS <= 0) {
            throw new IllegalArgumentException("ttlEpochS must be positive");
        }
    }
}
