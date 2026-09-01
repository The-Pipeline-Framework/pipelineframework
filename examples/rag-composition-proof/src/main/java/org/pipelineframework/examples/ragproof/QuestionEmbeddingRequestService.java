package org.pipelineframework.examples.ragproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.examples.ragproof.domain.Question;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public class QuestionEmbeddingRequestService implements ReactiveService<Question, EmbeddingRequest> {
    @Override public Uni<EmbeddingRequest> process(Question input) {
        return Uni.createFrom().item(new EmbeddingRequest(input.questionId(), input.text()));
    }
}
