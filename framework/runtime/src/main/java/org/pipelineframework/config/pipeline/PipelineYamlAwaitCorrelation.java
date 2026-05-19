package org.pipelineframework.config.pipeline;

/**
 * Correlation configuration for an await step.
 *
 * @param strategy correlation strategy, for example {@code interactionId} or {@code signedResumeToken}
 */
public record PipelineYamlAwaitCorrelation(String strategy) {
    public PipelineYamlAwaitCorrelation {
        if (strategy == null || strategy.isBlank()) {
            strategy = "interactionId";
        }
    }
}
