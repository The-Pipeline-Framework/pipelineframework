package org.pipelineframework.connector.mcp;

public record OperationResultObservation(
    String binding,
    String operation,
    String kind,
    int majorVersion,
    String outcome,
    String code,
    String resultType,
    String resultJson
) {
}
