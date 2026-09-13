package org.pipelineframework.processor.ir;

/**
 * Binding for local/in-process client step generation.
 *
 * @param model the pipeline step model being bound
 */
public record LocalBinding(PipelineStepModel model) implements PipelineBinding {
}
