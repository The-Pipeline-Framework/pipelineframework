package org.pipelineframework.awaitable.dto;

import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;

/**
 * Maps await runtime records to transport DTOs.
 */
public final class AwaitDtoMapper {
    private AwaitDtoMapper() {
    }

    public static AwaitCompletionResponseDto toCompletionResponse(AwaitCompletionResult result) {
        AwaitInteractionRecord record = result.record();
        return new AwaitCompletionResponseDto(
            record.interactionId(),
            record.executionId(),
            record.stepId(),
            record.status(),
            result.duplicate());
    }

    public static AwaitInteractionDto toDto(AwaitInteractionRecord record) {
        return new AwaitInteractionDto(
            record.interactionId(),
            record.correlationId(),
            record.executionId(),
            record.stepId(),
            record.stepIndex(),
            record.outputType(),
            record.status(),
            record.requestPayload(),
            record.assignee(),
            record.group(),
            record.transportType(),
            record.transportMetadata(),
            record.deadlineEpochMs(),
            record.createdAtEpochMs(),
            record.updatedAtEpochMs());
    }
}
