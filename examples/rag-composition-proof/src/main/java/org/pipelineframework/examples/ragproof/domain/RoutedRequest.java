package org.pipelineframework.examples.ragproof.domain;

public sealed interface RoutedRequest permits Document, Question {
}
