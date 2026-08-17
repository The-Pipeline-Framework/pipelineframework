package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import org.pipelineframework.repository.PayloadReference;

/** Specialized list/read object-source operation family. */
public interface ObjectSourceOperation extends ConnectorOperation {
    default CompletionStage<MaterializedPayload> materialize(PayloadReference reference, long maxBytes) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "object source operation does not support payload materialization: " + id()));
    }
}
