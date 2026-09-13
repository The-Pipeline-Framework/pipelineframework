package org.pipelineframework.objectingest;

/**
 * Adapts object-ingest domain inputs to the generated pipeline input boundary.
 *
 * <p>Local deployments consume domain objects directly. Remote orchestrator client steps consume the
 * transport type generated for the first business step, so deployment generates this adapter when an
 * object-ingest source feeds a remote pipeline boundary.
 */
public interface ObjectIngestInputAdapter<D, P> {

    Class<D> domainType();

    /**
     * Converts a non-null mapped ingest domain item into the generated pipeline input type.
     *
     * @param item non-null domain item returned by the object snapshot mapper
     * @return non-null item accepted by the generated pipeline input boundary
     * @throws IllegalArgumentException if the domain item cannot be converted to pipeline input
     * @throws NullPointerException if {@code item} is null
     */
    P toPipelineInput(D item);
}
