package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Input command for creating or resolving an async execution.
 *
 * @param tenantId tenant identifier
 * @param executionKey deduplication key for this submission
 * @param inputPayload original orchestrator input payload
 * @param nowEpochMs current timestamp in epoch milliseconds
 * @param ttlEpochS expiration timestamp in epoch seconds
 */
public record ExecutionCreateCommand(
    String tenantId,
    String executionKey,
    Object inputPayload,
    long nowEpochMs,
    long ttlEpochS
) {
    public ExecutionCreateCommand {
        Objects.requireNonNull(executionKey, "ExecutionCreateCommand.executionKey must not be null");
    }
}
