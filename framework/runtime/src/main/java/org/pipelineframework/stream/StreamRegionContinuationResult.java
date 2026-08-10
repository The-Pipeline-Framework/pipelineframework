package org.pipelineframework.stream;

import java.util.List;

/**
 * Ordinary typed result of one bounded producer continuation transition.
 *
 * <p>Generated implementations declare a concrete canonical item type for {@link #items()}, so
 * the existing transition payload codec retains element types across a remote worker boundary.
 */
public interface StreamRegionContinuationResult {

    List<?> items();

    OpaqueSourceCheckpoint nextCheckpoint();

    boolean endOfSource();
}
