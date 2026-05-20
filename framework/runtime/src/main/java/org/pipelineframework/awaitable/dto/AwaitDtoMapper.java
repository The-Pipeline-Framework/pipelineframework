package org.pipelineframework.awaitable.dto;

import java.util.Objects;

import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;

/**
 * Maps await runtime records to transport DTOs.
 */
public final class AwaitDtoMapper {
    private AwaitDtoMapper() {
    }

    public static AwaitCompletionResponseDto toCompletionResponse(AwaitCompletionResult result) {
        AwaitInteractionRecord record = Objects.requireNonNull(
            Objects.requireNonNull(result, "result must not be null").record(),
            "result.record must not be null");
        Objects.requireNonNull(record.interactionId(), "record.interactionId must not be null");
        Objects.requireNonNull(record.executionId(), "record.executionId must not be null");
        Objects.requireNonNull(record.stepId(), "record.stepId must not be null");
        Objects.requireNonNull(record.status(), "record.status must not be null");
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
