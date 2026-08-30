package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.context.PipelineContext;

/**
 * Durable wrapper for async execution input payload plus original shape metadata.
 *
 * @param shape original submission shape
 * @param payload canonicalized payload
 * @param pipelineContext request policy captured at durable admission
 */
public record ExecutionInputSnapshot(
    ExecutionInputShape shape,
    Object payload,
    Optional<PipelineContext> pipelineContext
) {
    public ExecutionInputSnapshot(ExecutionInputShape shape, Object payload) {
        this(shape, payload, Optional.empty());
    }

    public ExecutionInputSnapshot(ExecutionInputShape shape, Object payload, PipelineContext pipelineContext) {
        this(shape, payload, Optional.ofNullable(pipelineContext));
    }

    public ExecutionInputSnapshot {
        Objects.requireNonNull(shape, "ExecutionInputSnapshot.shape must not be null");
        pipelineContext = Optional.ofNullable(pipelineContext)
            .orElseGet(Optional::empty)
            .filter(ExecutionInputSnapshot::hasRequestPolicy);
    }

    private static boolean hasRequestPolicy(PipelineContext context) {
        return context.versionTag() != null || context.replayMode() != null || context.cachePolicy() != null;
    }
}
