package org.pipelineframework.examples.rag.query;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.connector.embedding.EmbeddingResult;
import org.pipelineframework.connector.vector.VectorSearchRequest;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public final class VectorSearchRequestService implements ReactiveService<EmbeddingResult, VectorSearchRequest> {
    private final int limit;

    public VectorSearchRequestService(@ConfigProperty(name = "rag.query.top-k", defaultValue = "5") int limit) {
        if (limit <= 0) throw new IllegalArgumentException("rag.query.top-k must be positive");
        this.limit = limit;
    }

    @Override public Uni<VectorSearchRequest> process(EmbeddingResult embedding) {
        return Uni.createFrom().item(new VectorSearchRequest(
            embedding.itemId(), embedding.text(), embedding.values(), limit));
    }
}
