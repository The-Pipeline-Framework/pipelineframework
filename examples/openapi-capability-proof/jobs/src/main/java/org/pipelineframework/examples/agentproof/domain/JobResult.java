package org.pipelineframework.examples.agentproof.domain;

/** Final value preserves original business input alongside the completed provider status. */
public record JobResult(String jobId, String value, String status) {
    public JobResult {
        java.util.Objects.requireNonNull(jobId, "jobId");
        java.util.Objects.requireNonNull(value, "value");
        java.util.Objects.requireNonNull(status, "status");
    }
}
