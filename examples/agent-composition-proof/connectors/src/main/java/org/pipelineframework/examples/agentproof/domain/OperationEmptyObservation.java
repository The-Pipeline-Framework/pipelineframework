package org.pipelineframework.examples.agentproof.domain;

public record OperationEmptyObservation(
    String binding,
    String operation,
    String kind,
    int operationVersion,
    String outcome,
    String code
) {
}
