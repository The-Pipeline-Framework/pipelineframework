package org.pipelineframework.processor.ir;

/**
 * Common interface for transport-specific bindings.
 */
public interface PipelineBinding {
    /**
     * Gets the semantic model this binding is based on.
     *
     * @return the pipeline step model
     */
    PipelineStepModel model();

    /**
     * Gets the name of the service.
     *
     * @return the service name
     */
    default String serviceName() {
        return model().serviceName();
    }

    /**
     * Gets the package of the service.
     *
     * @return the service package
     */
    default String servicePackage() {
        return model().servicePackage();
    }

    /**
     * Gets the simple class name of the service.
     *
     * @return the service class simple name
     */
    default String serviceClassName() {
        return model().serviceClassName().simpleName();
    }
}
