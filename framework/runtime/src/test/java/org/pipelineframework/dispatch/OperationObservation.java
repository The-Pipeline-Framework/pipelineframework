package org.pipelineframework.dispatch;

public sealed interface OperationObservation {
    record Result(OperationResultObservation value) implements OperationObservation {
    }

    record Empty(OperationEmptyObservation value) implements OperationObservation {
    }
}
