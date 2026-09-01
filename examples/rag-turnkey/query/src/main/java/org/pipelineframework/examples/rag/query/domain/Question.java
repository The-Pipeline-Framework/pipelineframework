package org.pipelineframework.examples.rag.query.domain;

import java.util.Objects;

public record Question(String questionId, String text) {
    public Question {
        questionId = requireText(questionId, "question ID");
        text = requireText(text, "question text");
    }
    private static String requireText(String value, String label) {
        String normalized = Objects.requireNonNull(value, label + " must not be null").trim();
        if (normalized.isEmpty()) throw new IllegalArgumentException(label + " must not be blank");
        return normalized;
    }
}
