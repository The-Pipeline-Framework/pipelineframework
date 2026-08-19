package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;
import org.pipelineframework.repository.PayloadReference;

/** Provider-neutral capability for loading bounded bytes from a portable payload reference. */
@FunctionalInterface
public interface PayloadMaterializer {
    /**
     * Materializes at most {@code maxBytes}. Implementations must close provider resources before
     * completing the returned stage and must fail when the declared or actual size exceeds the bound.
     */
    CompletionStage<MaterializedPayload> materialize(PayloadReference reference, long maxBytes);
}
