package org.pipelineframework.examples.ragproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.embedding.EmbeddingResult;
import org.pipelineframework.connector.vector.VectorUpsertRequest;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public class VectorUpsertRequestService implements ReactiveService<EmbeddingResult, VectorUpsertRequest> {
    @Override public Uni<VectorUpsertRequest> process(EmbeddingResult input) {
        return Uni.createFrom().item(new VectorUpsertRequest(input.itemId(), input.text(), input.values()));
    }
}
