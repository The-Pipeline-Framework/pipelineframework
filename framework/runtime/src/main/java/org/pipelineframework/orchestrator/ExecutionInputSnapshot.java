package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Durable wrapper for async execution input payload plus original shape metadata.
 *
 * @param shape original submission shape
 * @param payload canonicalized payload
 */
public record ExecutionInputSnapshot(
    ExecutionInputShape shape,
    Object payload
) {
    public ExecutionInputSnapshot {
        Objects.requireNonNull(shape, "ExecutionInputSnapshot.shape must not be null");
    }
}
