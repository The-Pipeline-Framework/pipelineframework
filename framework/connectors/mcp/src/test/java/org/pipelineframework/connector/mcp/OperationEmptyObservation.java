package org.pipelineframework.connector.mcp;

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
