package org.pipelineframework.examples.ragproof.domain;

public record Answer(String questionId, String text) implements RagResult {
}
