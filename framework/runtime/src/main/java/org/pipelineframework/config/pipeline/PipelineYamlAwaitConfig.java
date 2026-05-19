package org.pipelineframework.config.pipeline;

/**
 * Await-step configuration parsed from pipeline.yaml.
 *
 * @param correlation correlation configuration
 * @param transport transport adapter configuration
 */
public record PipelineYamlAwaitConfig(
    PipelineYamlAwaitCorrelation correlation,
    PipelineYamlAwaitTransport transport
) {
    public PipelineYamlAwaitConfig {
        correlation = correlation == null ? new PipelineYamlAwaitCorrelation("interactionId") : correlation;
        if (transport == null) {
            throw new IllegalArgumentException("await.transport must be defined");
        }
    }
}
