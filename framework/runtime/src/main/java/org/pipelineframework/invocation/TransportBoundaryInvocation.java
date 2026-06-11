package org.pipelineframework.invocation;

/**
 * Marker for generated clients and worker adapters that cross a remote transport boundary.
 */
public interface TransportBoundaryInvocation {

    /**
     * @return stable diagnostic metadata for this transport boundary
     */
    TransportBoundaryDescriptor transportBoundary();
}
