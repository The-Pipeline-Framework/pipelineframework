package org.pipelineframework.examples.rag.indexer;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.embedding.EmbeddingResult;
import org.pipelineframework.connector.vector.VectorUpsertRequest;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public final class VectorUpsertRequestService implements ReactiveService<EmbeddingResult, VectorUpsertRequest> {
    @Override public Uni<VectorUpsertRequest> process(EmbeddingResult embedding) {
        return Uni.createFrom().item(new VectorUpsertRequest(embedding.itemId(), embedding.text(), embedding.values()));
    }
}
