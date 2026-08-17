/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.pipelineframework.invocation;

import java.util.List;
import org.pipelineframework.step.NonRetryableException;

/** Explicit failure raised before a recursive invocation exceeds its configured depth. */
public final class PipelineRecursionLimitExceededException extends NonRetryableException {
    private final String definitionId;
    private final String callsiteId;
    private final int attemptedDepth;
    private final int maximumDepth;
    private final List<PipelineInvocationContext.Frame> parentPath;

    PipelineRecursionLimitExceededException(
        String definitionId,
        String callsiteId,
        int attemptedDepth,
        int maximumDepth,
        List<PipelineInvocationContext.Frame> parentPath
    ) {
        super("Recursive pipeline invocation limit exceeded at " + definitionId + ":" + callsiteId
            + ": attempted depth " + attemptedDepth + ", configured maximum " + maximumDepth);
        this.definitionId = definitionId;
        this.callsiteId = callsiteId;
        this.attemptedDepth = attemptedDepth;
        this.maximumDepth = maximumDepth;
        this.parentPath = List.copyOf(parentPath);
    }

    public String definitionId() {
        return definitionId;
    }

    public String callsiteId() {
        return callsiteId;
    }

    public int attemptedDepth() {
        return attemptedDepth;
    }

    public int maximumDepth() {
        return maximumDepth;
    }

    public List<PipelineInvocationContext.Frame> parentPath() {
        return parentPath;
    }
}
