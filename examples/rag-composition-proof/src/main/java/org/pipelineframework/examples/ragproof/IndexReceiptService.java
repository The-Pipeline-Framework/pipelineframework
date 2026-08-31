package org.pipelineframework.examples.ragproof;

import java.util.List;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.vector.VectorUpsertResult;
import org.pipelineframework.examples.ragproof.domain.IndexReceipt;
import org.pipelineframework.service.ReactiveStreamingClientService;

@ApplicationScoped
public class IndexReceiptService implements ReactiveStreamingClientService<VectorUpsertResult, IndexReceipt> {
    @Override public Uni<IndexReceipt> process(Multi<VectorUpsertResult> input) {
        return input.collect().asList().onItem().transform(IndexReceiptService::receipt);
    }

    private static IndexReceipt receipt(List<VectorUpsertResult> results) {
        if (results.isEmpty()) throw new IllegalArgumentException("at least one indexed chunk is required");
        String documentId = results.getFirst().itemId().split("#", 2)[0];
        if (results.stream().anyMatch(result -> !result.itemId().startsWith(documentId + "#"))) {
            throw new IllegalArgumentException("all indexed chunks must belong to one document");
        }
        return new IndexReceipt(documentId, results.size());
    }
}
