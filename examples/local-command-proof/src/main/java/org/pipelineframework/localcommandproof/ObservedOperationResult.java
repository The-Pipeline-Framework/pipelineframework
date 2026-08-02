package org.pipelineframework.localcommandproof;

/**
 * Typed evidence returned by the local command proof fixture.
 */
public record ObservedOperationResult(
    String operationId,
    String confirmation,
    String workerThreadName
) {
}
