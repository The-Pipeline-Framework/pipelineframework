package org.pipelineframework.processor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;

/**
 * Abstract base class providing common functionality for annotation processing tools.
 * Contains utility methods for annotation processing operations.
 * <p>
 * Default constructor is provided by the abstract processor.
 */
public abstract class AbstractProcessingTool extends AbstractProcessor {
    /**
     * Initialises a new AbstractProcessingTool instance.
     *
     * No additional initialisation is performed beyond the superclass construction.
     */
    public AbstractProcessingTool() {
    }

    /**
     * Initialises the processor with the supplied processing environment.
     *
     * @param processingEnv the processing environment to be used by this processor
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }
}