package org.pipelineframework.connector.mcp;

public sealed interface OperationObservation {
    record Result(OperationResultObservation value) implements OperationObservation {
    }

    record Empty(OperationEmptyObservation value) implements OperationObservation {
    }
}
