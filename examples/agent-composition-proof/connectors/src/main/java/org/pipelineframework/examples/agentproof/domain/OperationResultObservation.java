package org.pipelineframework.examples.agentproof.domain;

public record OperationResultObservation(
    String binding,
    String operation,
    String kind,
    int operationVersion,
    String outcome,
    String code,
    String resultType,
    String resultJson
) {
}
