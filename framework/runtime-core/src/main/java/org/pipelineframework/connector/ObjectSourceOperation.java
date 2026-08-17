package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import org.pipelineframework.repository.PayloadReference;

/** Specialized list/read object-source operation family. */
public interface ObjectSourceOperation extends ConnectorOperation {
    /**
     * Materializes a referenced payload up to the specified byte limit.
     *
     * @param reference the payload to materialize
     * @param maxBytes the maximum number of bytes to materialize
     * @return the materialized payload
     */
    default CompletionStage<MaterializedPayload> materialize(PayloadReference reference, long maxBytes) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "object source operation does not support payload materialization: " + id()));
    }
}
