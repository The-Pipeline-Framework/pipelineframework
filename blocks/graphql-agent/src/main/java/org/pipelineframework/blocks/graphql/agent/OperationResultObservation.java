package org.pipelineframework.blocks.graphql.agent;

public record OperationResultObservation(
    String binding,
    String operation,
    String kind,
    int operationVersion,
    String outcome,
    String code,
    String argumentsJson,
    String contextJson,
    String resultType,
    String resultJson
) {
}
