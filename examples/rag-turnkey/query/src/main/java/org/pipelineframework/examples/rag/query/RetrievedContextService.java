package org.pipelineframework.examples.rag.query;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.vector.VectorSearchResult;
import org.pipelineframework.examples.rag.query.domain.RetrievedChunk;
import org.pipelineframework.examples.rag.query.domain.RetrievedContext;
import org.pipelineframework.examples.rag.support.ChunkId;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public final class RetrievedContextService implements ReactiveService<VectorSearchResult, RetrievedContext> {
    @Override public Uni<RetrievedContext> process(VectorSearchResult result) {
        var chunks = result.matches().stream().map(match -> {
            ChunkId id = ChunkId.decode(match.itemId());
            return new RetrievedChunk(id.sourceId(), match.itemId(), match.content(), match.score());
        }).toList();
        return Uni.createFrom().item(new RetrievedContext(result.queryId(), result.queryText(), chunks));
    }
}
