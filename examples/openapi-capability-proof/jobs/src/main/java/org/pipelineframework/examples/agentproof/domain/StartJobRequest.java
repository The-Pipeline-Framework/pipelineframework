package org.pipelineframework.examples.agentproof.domain;

/** Canonical business input deliberately has no endpoint, credential, or callback URI. */
public record StartJobRequest(String jobId, String value) {
    public StartJobRequest {
        java.util.Objects.requireNonNull(jobId, "jobId");
        java.util.Objects.requireNonNull(value, "value");
        if (jobId.isBlank()) throw new IllegalArgumentException("jobId must not be blank");
    }
}
