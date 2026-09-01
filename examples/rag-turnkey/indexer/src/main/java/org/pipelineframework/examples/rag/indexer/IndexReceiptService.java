package org.pipelineframework.examples.rag.indexer;

import java.util.List;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.vector.VectorUpsertResult;
import org.pipelineframework.examples.rag.indexer.domain.IndexReceipt;
import org.pipelineframework.examples.rag.support.ChunkId;
import org.pipelineframework.service.ReactiveStreamingClientService;

@ApplicationScoped
public final class IndexReceiptService implements ReactiveStreamingClientService<VectorUpsertResult, IndexReceipt> {
    @Override public Uni<IndexReceipt> process(Multi<VectorUpsertResult> input) {
        return input.collect().asList().map(IndexReceiptService::receipt);
    }

    static IndexReceipt receipt(List<VectorUpsertResult> results) {
        if (results.isEmpty()) throw new IllegalArgumentException("at least one indexed chunk is required");
        String sourceId = ChunkId.decode(results.getFirst().itemId()).sourceId();
        if (results.stream().map(VectorUpsertResult::itemId).map(ChunkId::decode)
            .map(ChunkId::sourceId).anyMatch(candidate -> !candidate.equals(sourceId))) {
            throw new IllegalArgumentException("all indexed chunks must belong to one source");
        }
        return new IndexReceipt(sourceId, results.size());
    }
}
