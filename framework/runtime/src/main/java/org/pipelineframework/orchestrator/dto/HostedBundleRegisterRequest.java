package org.pipelineframework.orchestrator.dto;

/**
 * Local/dev hosted bundle registration request.
 *
 * @param artifactPath absolute path to a generated pipeline bundle JAR
 */
public record HostedBundleRegisterRequest(String artifactPath) {
}
