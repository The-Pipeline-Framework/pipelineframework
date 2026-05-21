package org.pipelineframework.config.pipeline;

/**
 * Await dispatch configuration parsed from pipeline.yaml.
 *
 * @param mode dispatch mode; defaults to {@code single}
 */
public record PipelineYamlAwaitDispatch(String mode) {
    public PipelineYamlAwaitDispatch {
        mode = mode == null || mode.isBlank() ? "single" : mode.trim();
    }
}
