/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import org.pipelineframework.config.ParallelismPolicy;

/** Focused runtime seam for pipeline-run lifecycle ownership. */
public interface PipelineRunTelemetry {
    /** Returns a context for execution that does not own a pipeline-run telemetry lifecycle. */
    public static PipelineRunContext nonOwningContext() {
        return PipelineRunContext.disabled();
    }

    PipelineRunContext startRun(Object input, int stepCount, ParallelismPolicy policy, int maxConcurrency);
    Object instrumentInput(Object input, PipelineRunContext context);
    Object instrumentRunCompletion(Object publisher, PipelineRunContext context);
    void abortRun(PipelineRunContext context, Throwable failure);
    void abortActiveRun(Throwable failure);
}
