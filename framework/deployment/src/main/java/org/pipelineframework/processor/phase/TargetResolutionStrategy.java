package org.pipelineframework.processor.phase;

import java.util.Set;

import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.TransportMode;

/**
 * Strategy for resolving generation targets by transport mode.
 */
@FunctionalInterface
public interface TargetResolutionStrategy {

    /**
     * Resolve generation targets for the given transport mode.
     *
     * @param transportMode transport mode to resolve against; must not be {@code null}
     * @return a non-null, unmodifiable set of {@link GenerationTarget}; empty when no targets are applicable
     */
    Set<GenerationTarget> resolve(TransportMode transportMode);
}
