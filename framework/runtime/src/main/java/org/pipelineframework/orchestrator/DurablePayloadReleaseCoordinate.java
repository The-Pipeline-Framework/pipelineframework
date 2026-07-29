package org.pipelineframework.orchestrator;

/** Immutable identity of the release that owns canonical durable payload bindings. */
public record DurablePayloadReleaseCoordinate(String pipelineId, String contractVersion, String releaseVersion) {
    public DurablePayloadReleaseCoordinate {
        if (pipelineId == null || pipelineId.isBlank() || contractVersion == null || contractVersion.isBlank()
            || releaseVersion == null || releaseVersion.isBlank()) {
            throw new IllegalArgumentException("Durable payload release coordinate fields must not be blank");
        }
    }
}
