package org.pipelineframework.examples.ragproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.examples.ragproof.domain.RagInput;
import org.pipelineframework.examples.ragproof.domain.RoutedRequest;
import org.pipelineframework.service.ReactiveService;

/** Makes the root union available to both independently accepted branches. */
@ApplicationScoped
@PipelineStep
public class RagRequestService implements ReactiveService<RagInput, RoutedRequest> {
    @Override public Uni<RoutedRequest> process(RagInput input) {
        return switch (input.kind()) {
            case "index" -> Uni.createFrom().item(new org.pipelineframework.examples.ragproof.domain.Document(
                input.id(), input.text()));
            case "ask" -> Uni.createFrom().item(new org.pipelineframework.examples.ragproof.domain.Question(
                input.id(), input.text()));
            default -> Uni.createFrom().failure(new IllegalArgumentException(
                "RAG input kind must be 'index' or 'ask': " + input.kind()));
        };
    }
}
