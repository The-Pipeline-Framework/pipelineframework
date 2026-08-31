package org.pipelineframework.examples.ragproof.domain;

public record Question(String questionId, String text) implements RoutedRequest {
}
