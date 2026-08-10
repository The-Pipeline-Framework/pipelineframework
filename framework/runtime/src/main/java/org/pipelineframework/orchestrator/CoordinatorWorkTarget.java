package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Typed target carried by the existing coordinator work envelope.
 *
 * <p>The dispatcher remains single-substrate. A target only selects the durable record to claim;
 * it does not introduce a scheduler or transport protocol.
 */
public record CoordinatorWorkTarget(Kind kind, String targetId) {

    public enum Kind {
        EXECUTION,
        AWAIT_INTERACTION,
        STREAM_REGION
    }

    public CoordinatorWorkTarget {
        kind = Objects.requireNonNull(kind, "work target kind must not be null");
        targetId = Objects.requireNonNull(targetId, "work target id must not be null").trim();
        if (targetId.isBlank()) {
            throw new IllegalArgumentException("work target id must not be blank");
        }
    }

    public static CoordinatorWorkTarget execution(String executionId) {
        return new CoordinatorWorkTarget(Kind.EXECUTION, executionId);
    }

    public static CoordinatorWorkTarget awaitInteraction(String interactionId) {
        return new CoordinatorWorkTarget(Kind.AWAIT_INTERACTION, interactionId);
    }

    /**
     * Targets one bounded producer-owned stream region. The coordinator claims the region before
     * arranging its ordinary transition-worker invocation.
     */
    public static CoordinatorWorkTarget streamRegion(String regionId) {
        return new CoordinatorWorkTarget(Kind.STREAM_REGION, regionId);
    }

    public boolean execution() {
        return kind == Kind.EXECUTION;
    }

    public boolean awaitInteraction() {
        return kind == Kind.AWAIT_INTERACTION;
    }

    public boolean streamRegion() {
        return kind == Kind.STREAM_REGION;
    }
}
