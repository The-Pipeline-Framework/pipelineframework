package org.pipelineframework.connector.vector;

import org.pipelineframework.connector.CommandOperation;

/** Provider-neutral Command operation for one vector upsert. */
public interface VectorUpsertCommandOperation<C>
    extends CommandOperation<VectorUpsertRequest, C, VectorUpsertResult> {
    @Override
    default String id() {
        return "upsert";
    }
}
