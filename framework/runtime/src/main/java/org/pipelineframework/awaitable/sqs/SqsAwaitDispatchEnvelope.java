package org.pipelineframework.awaitable.sqs;

import java.util.Map;

import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitStepDescriptor;

/**
 * Framework-owned SQS await request envelope.
 */
public record SqsAwaitDispatchEnvelope(
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
    public SqsAwaitDispatchEnvelope {
        transportMetadata = transportMetadata == null ? Map.of() : Map.copyOf(transportMetadata);
    }

    public static SqsAwaitDispatchEnvelope from(
        AwaitStepDescriptor descriptor,
        AwaitInteractionRecord interaction,
        Object payload,
        String resumeToken,
        Map<String, Object> transportMetadata) {
        return new SqsAwaitDispatchEnvelope(
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
