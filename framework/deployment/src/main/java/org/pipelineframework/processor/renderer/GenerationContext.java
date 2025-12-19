package org.pipelineframework.processor.renderer;

import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.JavaFileObject;

/**
 * Context for code generation operations, containing processing environment and output file information.
 */
public class GenerationContext {
    private final ProcessingEnvironment processingEnv;
    private final JavaFileObject builderFile;

    /**
     * Creates a new GenerationContext instance.
     *
     * @param processingEnv the processing environment
     * @param builderFile the Java file object for the generated source
     */
    public GenerationContext(ProcessingEnvironment processingEnv, JavaFileObject builderFile) {
        this.processingEnv = processingEnv;
        this.builderFile = builderFile;
    }

    /**
     * Gets the processing environment.
     *
     * @return the processing environment
     */
    public ProcessingEnvironment getProcessingEnv() {
        return processingEnv;
    }

    /**
     * Gets the builder file object for the output.
     *
     * @return the Java file object
     */
    public JavaFileObject getBuilderFile() {
        return builderFile;
    }
}