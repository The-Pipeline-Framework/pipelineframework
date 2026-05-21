package org.pipelineframework.awaitable.kafka;

import java.util.Map;

import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitStepDescriptor;

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

    public static KafkaAwaitDispatchEnvelope from(
        AwaitStepDescriptor descriptor,
        AwaitInteractionRecord interaction,
        Object payload,
        String resumeToken,
        Map<String, Object> transportMetadata) {
        return new KafkaAwaitDispatchEnvelope(
            interaction.tenantId(),
            interaction.executionId(),
            interaction.interactionId(),
            interaction.correlationId(),
            interaction.stepId(),
            interaction.deadlineEpochMs(),
            descriptor.inputType(),
            descriptor.outputType(),
            resumeToken,
            payload,
            transportMetadata);
    }
}
