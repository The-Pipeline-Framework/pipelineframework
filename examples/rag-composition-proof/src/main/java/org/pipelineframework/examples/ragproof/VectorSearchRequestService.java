package org.pipelineframework.examples.ragproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.embedding.EmbeddingResult;
import org.pipelineframework.connector.vector.VectorSearchRequest;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public class VectorSearchRequestService implements ReactiveService<EmbeddingResult, VectorSearchRequest> {
    @Override public Uni<VectorSearchRequest> process(EmbeddingResult input) {
        return Uni.createFrom().item(new VectorSearchRequest(input.itemId(), input.text(), input.values(), 3));
    }
}
