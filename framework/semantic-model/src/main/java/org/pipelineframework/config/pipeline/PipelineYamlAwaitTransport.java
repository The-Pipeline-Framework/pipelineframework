package org.pipelineframework.config.pipeline;

import java.util.Map;

/**
 * Transport-specific configuration for an await step.
 *
 * <p>The framework owns orchestration and correlation. Adapter modules own the shape of the
 * transport-specific maps.</p>
 *
 * @param type transport adapter type
 * @param config raw transport config map
 */
public record PipelineYamlAwaitTransport(String type, Map<String, Object> config) {
    public PipelineYamlAwaitTransport {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("await.transport.type must be defined");
        }
        config = config == null ? Map.of() : Map.copyOf(config);
    }
}
