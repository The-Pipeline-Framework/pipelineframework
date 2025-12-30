package org.pipelineframework.processor.renderer;

import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.JavaFileObject;

/**
 * Context for code generation operations, containing processing environment and output file information.
 *
 * @param processingEnv Gets the processing environment.
 * @param builderFile Gets the builder file object for the output.
 */
public record GenerationContext(ProcessingEnvironment processingEnv, JavaFileObject builderFile) {
    /**
     * Creates a new GenerationContext instance.
     */
    public GenerationContext {
    }
}