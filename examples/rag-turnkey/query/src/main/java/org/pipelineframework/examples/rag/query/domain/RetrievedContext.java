package org.pipelineframework.examples.rag.query.domain;

import java.util.List;
import java.util.Objects;

public record RetrievedContext(String questionId, String question, List<RetrievedChunk> chunks) {
    public RetrievedContext {
        questionId = Objects.requireNonNull(questionId, "question ID must not be null");
        question = Objects.requireNonNull(question, "question must not be null");
        chunks = List.copyOf(Objects.requireNonNull(chunks, "retrieved chunks must not be null"));
    }
}
