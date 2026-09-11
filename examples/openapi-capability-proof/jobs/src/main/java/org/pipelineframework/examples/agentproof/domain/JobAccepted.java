package org.pipelineframework.examples.agentproof.domain;

/** Immediate provider acknowledgement; it is not the final pipeline result. */
public record JobAccepted(String jobId, boolean accepted) {
    public JobAccepted {
        java.util.Objects.requireNonNull(jobId, "jobId");
        if (!accepted) {
            throw new IllegalArgumentException("Successful job acknowledgement must confirm acceptance");
        }
    }
}
