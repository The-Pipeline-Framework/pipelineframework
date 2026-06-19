package org.pipelineframework.query;

import io.smallrye.mutiny.Uni;

/**
 * Application-supplied connector that performs one decision-affecting external read.
 */
public interface QueryConnector<I, O> {
    String connectorName();

    Uni<O> execute(QueryRequest<I> request);
}
