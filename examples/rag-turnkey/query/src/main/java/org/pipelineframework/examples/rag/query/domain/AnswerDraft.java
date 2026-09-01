package org.pipelineframework.examples.rag.query.domain;

import java.util.List;
import java.util.Objects;

public record AnswerDraft(String text, List<String> citedChunkIds) {
    public AnswerDraft {
        text = Objects.requireNonNull(text, "answer text must not be null");
        citedChunkIds = List.copyOf(Objects.requireNonNull(citedChunkIds, "cited chunk IDs must not be null"));
    }
}
