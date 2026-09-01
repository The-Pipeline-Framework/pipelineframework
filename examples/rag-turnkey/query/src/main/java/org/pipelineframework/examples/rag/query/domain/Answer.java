package org.pipelineframework.examples.rag.query.domain;

import java.util.List;
import java.util.Objects;

public record Answer(String text, List<Citation> citations) {
    public Answer {
        text = Objects.requireNonNull(text, "answer text must not be null");
        citations = List.copyOf(Objects.requireNonNull(citations, "citations must not be null"));
    }
}
