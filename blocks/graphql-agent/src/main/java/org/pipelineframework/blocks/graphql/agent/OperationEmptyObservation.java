package org.pipelineframework.blocks.graphql.agent;

public record OperationEmptyObservation(
    String binding,
    String operation,
    String kind,
    int operationVersion,
    String outcome,
    String code,
    String argumentsJson,
    String contextJson
) {
}
