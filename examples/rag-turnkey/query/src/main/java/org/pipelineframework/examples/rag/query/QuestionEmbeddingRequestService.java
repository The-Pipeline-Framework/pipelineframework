package org.pipelineframework.examples.rag.query;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.examples.rag.query.domain.Question;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public final class QuestionEmbeddingRequestService implements ReactiveService<Question, EmbeddingRequest> {
    @Override public Uni<EmbeddingRequest> process(Question question) {
        return Uni.createFrom().item(new EmbeddingRequest(question.questionId(), question.text()));
    }
}
