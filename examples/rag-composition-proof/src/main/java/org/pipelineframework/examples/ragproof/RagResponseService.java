package org.pipelineframework.examples.ragproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.examples.ragproof.domain.RagResponse;
import org.pipelineframework.examples.ragproof.domain.RagResult;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public class RagResponseService implements ReactiveService<RagResult, RagResponse> {
    @Override public Uni<RagResponse> process(RagResult input) {
        return Uni.createFrom().item(new RagResponse(input));
    }
}
