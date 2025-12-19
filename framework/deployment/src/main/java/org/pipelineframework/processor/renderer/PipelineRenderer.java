package org.pipelineframework.processor.renderer;

import java.io.IOException;

import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepIR;

/**
 * Interface for pipeline renderers that generate specific artifacts based on PipelineStepIR.
 */
public interface PipelineRenderer {
    /**
     * Gets the generation target for this renderer.
     *
     * @return the generation target
     */
    GenerationTarget target();

    /**
     * Renders the artifact based on the provided IR.
     *
     * @param ir the pipeline step IR containing semantic information
     * @param ctx the generation context containing processing environment and output file
     * @throws IOException if an error occurs during rendering
     */
    void render(PipelineStepIR ir, GenerationContext ctx) throws IOException;
}