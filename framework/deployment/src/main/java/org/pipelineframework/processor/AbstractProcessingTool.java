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
     * Creates a new AbstractProcessingTool instance.
     * Default constructor provided by AbstractProcessor.
     */
    public AbstractProcessingTool() {
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }
}
