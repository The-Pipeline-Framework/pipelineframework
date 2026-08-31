package org.pipelineframework.examples.ragproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.examples.ragproof.domain.Chunk;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public class ChunkEmbeddingRequestService implements ReactiveService<Chunk, EmbeddingRequest> {
    @Override public Uni<EmbeddingRequest> process(Chunk input) {
        return Uni.createFrom().item(new EmbeddingRequest(input.chunkId(), input.content()));
    }
}
