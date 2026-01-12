package org.pipelineframework.processor.phase;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;

/**
 * Resolves generation targets based on configuration and annotation settings.
 * This phase determines which targets (gRPC, REST, client, server) should be generated.
 */
public class TargetResolutionPhase implements PipelineCompilationPhase {

    @Override
    public String name() {
        return "Target Resolution Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // This phase now focuses on target resolution tasks that build upon previous phases
    }
}