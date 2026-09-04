package org.pipelineframework.connector.mcp;

public record OperationEmptyObservation(
    String binding,
    String operation,
    String kind,
    int majorVersion,
    String outcome,
    String code
) {
}
