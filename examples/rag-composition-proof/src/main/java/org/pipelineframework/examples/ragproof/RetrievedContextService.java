package org.pipelineframework.examples.ragproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.vector.VectorMatch;
import org.pipelineframework.connector.vector.VectorSearchResult;
import org.pipelineframework.examples.ragproof.domain.RetrievedContext;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public class RetrievedContextService implements ReactiveService<VectorSearchResult, RetrievedContext> {
    @Override public Uni<RetrievedContext> process(VectorSearchResult input) {
        return Uni.createFrom().item(new RetrievedContext(input.queryId(), input.queryText(),
            input.matches().stream().map(VectorMatch::content).toList()));
    }
}
