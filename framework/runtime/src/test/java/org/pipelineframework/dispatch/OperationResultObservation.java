package org.pipelineframework.dispatch;

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
