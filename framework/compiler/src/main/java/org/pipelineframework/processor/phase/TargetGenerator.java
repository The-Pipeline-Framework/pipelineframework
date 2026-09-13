package org.pipelineframework.processor.phase;

import java.io.IOException;

import org.pipelineframework.processor.ir.GenerationTarget;

/**
 * Strategy for generating artifacts for a specific target.
 */
public interface TargetGenerator {

    /**
     * @return target handled by this generator
     */
    GenerationTarget target();

    /**
     * Perform generation for the target.
     *
     * @param request generation request
     * @throws IOException when artifact writing fails
     */
    void generate(GenerationRequest request) throws IOException;
}
