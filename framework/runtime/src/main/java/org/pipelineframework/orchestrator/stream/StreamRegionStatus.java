package org.pipelineframework.orchestrator.stream;

/** Lifecycle of one bounded durable stream region. */
public enum StreamRegionStatus {
    ACTIVE,
    SOURCE_SEALED,
    COMPLETED,
    CANCELLED,
    FAILED;

    public boolean terminal() {
        return this == COMPLETED || this == CANCELLED || this == FAILED;
    }
}
