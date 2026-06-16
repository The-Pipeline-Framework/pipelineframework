package org.pipelineframework.awaitable.kafka;

import java.util.Map;

/**
 * Framework-owned Kafka await request envelope.
 */
public record KafkaAwaitDispatchEnvelope(
    String tenantId,
    String executionId,
    String interactionId,
    String correlationId,
    String stepId,
    long deadlineEpochMs,
    String inputType,
    String outputType,
    String resumeToken,
    Object requestPayload,
    Map<String, Object> transportMetadata
) {
    public KafkaAwaitDispatchEnvelope {
        transportMetadata = transportMetadata == null ? Map.of() : Map.copyOf(transportMetadata);
    }
}
