package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Runtime descriptor for one generated await step.
 *
 * @param stepId stable generated step id
 * @param inputType input domain type
 * @param outputType output domain type
 * @param cardinality pipeline cardinality shape
 * @param timeout maximum wait duration
 * @param correlationStrategy strategy used to derive adapter-visible correlation ids
 * @param transportType adapter type
 * @param transportConfig transport-specific adapter config
 * @param idempotencyKeyFields fields used to derive stable idempotency keys
 */
public record AwaitStepDescriptor(
    String stepId,
    String inputType,
    String outputType,
    String cardinality,
    Duration timeout,
    String correlationStrategy,
    String transportType,
    Map<String, Object> transportConfig,
    List<String> idempotencyKeyFields
) {
    public AwaitStepDescriptor(
        String stepId,
        String inputType,
        String outputType,
        Duration timeout,
        String correlationStrategy,
        String transportType,
        Map<String, Object> transportConfig,
        List<String> idempotencyKeyFields
    ) {
        this(
            stepId,
            inputType,
            outputType,
            "ONE_TO_ONE",
            timeout,
            correlationStrategy,
            transportType,
            transportConfig,
            idempotencyKeyFields);
    }

    public AwaitStepDescriptor {
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("stepId must not be blank");
        }
        if (inputType == null || inputType.isBlank()) {
            throw new IllegalArgumentException("inputType must not be blank");
        }
        if (outputType == null || outputType.isBlank()) {
            throw new IllegalArgumentException("outputType must not be blank");
        }
        cardinality = cardinality == null || cardinality.isBlank() ? "ONE_TO_ONE" : cardinality.trim();
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        if (transportType == null || transportType.isBlank()) {
            throw new IllegalArgumentException("transportType must not be blank");
        }
        correlationStrategy = correlationStrategy == null || correlationStrategy.isBlank()
            ? "interactionId"
            : correlationStrategy;
        transportConfig = transportConfig == null ? Map.of() : Map.copyOf(transportConfig);
        idempotencyKeyFields = idempotencyKeyFields == null ? List.of() : List.copyOf(idempotencyKeyFields);
    }
}
