package org.pipelineframework.representation.spi;

/** Host-defined lifecycle phase used solely for deterministic artifact ordering. */
public enum ArtifactPhase {
    PRE_MODEL,
    SOURCE,
    RESOURCE
}
