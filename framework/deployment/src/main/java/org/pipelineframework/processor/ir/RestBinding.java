package org.pipelineframework.processor.ir;

/**
 * Represents REST bindings for a pipeline step.
 *
 * @param model Reference to the semantic model this binding is based on
 * @param restPathOverride Optional REST path override from configuration
 */
public record RestBinding(
        PipelineStepModel model,
        String restPathOverride
) implements PipelineBinding {
    /**
     * Creates a new RestBinding instance.
     *
     * @param model the semantic model this binding is based on
     * @param restPathOverride the REST path override (optional)
     */
    public RestBinding {
        if (model == null) {
            throw new IllegalArgumentException("model cannot be null");
        }
    }
}
