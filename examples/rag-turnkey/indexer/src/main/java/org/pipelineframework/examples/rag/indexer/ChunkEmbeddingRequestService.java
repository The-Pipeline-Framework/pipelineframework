package org.pipelineframework.examples.rag.indexer;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.examples.rag.indexer.domain.Chunk;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public final class ChunkEmbeddingRequestService implements ReactiveService<Chunk, EmbeddingRequest> {
    @Override public Uni<EmbeddingRequest> process(Chunk chunk) {
        return Uni.createFrom().item(new EmbeddingRequest(chunk.chunkId(), chunk.content()));
    }
}
