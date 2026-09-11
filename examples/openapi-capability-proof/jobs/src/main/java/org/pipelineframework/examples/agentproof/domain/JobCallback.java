package org.pipelineframework.examples.agentproof.domain;

/** Authenticated provider completion admitted by TPF's callback ingress. */
public record JobCallback(String jobId, String status) {
    public JobCallback {
        java.util.Objects.requireNonNull(jobId, "jobId");
        java.util.Objects.requireNonNull(status, "status");
    }
}
