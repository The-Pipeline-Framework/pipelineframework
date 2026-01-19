package org.pipelineframework.processor.renderer;

import java.io.IOException;

import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineBinding;

/**
 * Interface for pipeline renderers that generate specific artifacts based on PipelineStepModel and transport bindings.
 *
 * @param <T> the binding type rendered by this renderer
 */
public interface PipelineRenderer<T extends PipelineBinding> {
    /**
     * The generation target produced by this renderer.
     *
     * @return the generation target for this renderer
     */
    GenerationTarget target();

    /**
     * Renders the artifact based on the provided binding.
     *
     * @param binding the pipeline binding containing semantic information and transport bindings
     * @param ctx the generation context containing processing environment and output file
     * @throws IOException if an error occurs during rendering
     */
    void render(T binding, GenerationContext ctx) throws IOException;
}
