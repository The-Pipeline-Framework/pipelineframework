package org.pipelineframework.stream;

/**
 * Worker input for one ordinary, bounded producer continuation transition.
 *
 * <p>Generated implementations retain the concrete canonical source-input type. Region identity,
 * OCC version, lease and credits deliberately do not cross the transition-worker boundary.
 */
public interface StreamRegionContinuationInput {

    ResumableSourceDescriptor descriptor();

    OpaqueSourceCheckpoint checkpoint();

    int limit();
}
