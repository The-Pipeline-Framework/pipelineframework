package org.pipelineframework.orchestrator.dto;

/**
 * Response payload for accepted async execution submissions.
 *
 * @param executionId execution identifier
 * @param duplicate whether request resolved to an existing execution
 * @param statusUrl status endpoint URL suffix
 * @param submittedAtEpochMs accepted timestamp
 */
public record RunAsyncAcceptedDto(
    String executionId,
    boolean duplicate,
    String statusUrl,
    long submittedAtEpochMs
) {
}
