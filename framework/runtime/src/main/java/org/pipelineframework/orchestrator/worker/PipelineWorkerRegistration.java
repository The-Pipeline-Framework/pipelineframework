package org.pipelineframework.orchestrator.worker;

import java.util.Objects;

/**
 * Worker lifecycle registration command.
 */
public record PipelineWorkerRegistration(
    String tenantId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String workerId,
    String protocol,
    String endpoint,
    String artifactId,
    String artifactDigest
) {
    public PipelineWorkerRegistration {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(contractVersion, "contractVersion");
        Objects.requireNonNull(releaseVersion, "releaseVersion");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(protocol, "protocol");
        tenantId = requireNonBlank("tenantId", tenantId);
        pipelineId = requireNonBlank("pipelineId", pipelineId);
        contractVersion = requireNonBlank("contractVersion", contractVersion);
        releaseVersion = requireNonBlank("releaseVersion", releaseVersion);
        workerId = requireNonBlank("workerId", workerId);
        protocol = requireNonBlank("protocol", protocol).toLowerCase(java.util.Locale.ROOT);
        endpoint = endpoint == null ? "" : endpoint.trim();
        artifactId = artifactId == null ? "" : artifactId.trim();
        artifactDigest = artifactDigest == null ? "" : artifactDigest.trim();
    }

    private static String requireNonBlank(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
