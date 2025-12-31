package org.pipelineframework.processor.ir;

/**
 * Common interface for transport-specific bindings.
 */
public interface PipelineBinding {
    /**
 * Retrieve the semantic pipeline step model for this binding.
 *
 * @return the semantic PipelineStepModel that defines the bound pipeline step
 */
    PipelineStepModel model();

    /**
     * Obtain the service's configured name.
     *
     * @return the service name, or `null` if not specified
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
     * Obtain the simple (unqualified) class name of the service associated with this binding's model.
     *
     * @return the simple name of the service class
     */
    default String serviceClassName() {
        return model().serviceClassName().simpleName();
    }
}