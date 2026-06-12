package org.pipelineframework.orchestrator.release.dto;

/**
 * Local/dev release registration request.
 *
 * @param releaseDescriptorPath absolute path to pipeline-release.json
 */
public record HostedReleaseRegisterRequest(String releaseDescriptorPath) {
}
