package org.pipelineframework.orchestrator.worker.dto;

/**
 * Local/dev worker lifecycle registration request.
 *
 * @param workerId stable worker instance id
 * @param contractVersion contract version hosted by the worker
 * @param releaseVersion release version hosted by the worker
 * @param protocol selected worker protocol
 * @param endpoint worker endpoint or local descriptor
 * @param artifactId optional artifact id
 * @param artifactDigest optional artifact digest
 */
public record HostedWorkerRegisterRequest(
    String workerId,
    String contractVersion,
    String releaseVersion,
    String protocol,
    String endpoint,
    String artifactId,
    String artifactDigest
) {
}
