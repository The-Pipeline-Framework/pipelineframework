package org.pipelineframework.config.pipeline;

import java.util.Map;

/**
 * Query connector definition parsed from pipeline.yaml.
 */
public record PipelineYamlQuery(
    String id,
    String connector,
    String inputType,
    String outputType,
    String version,
    Map<String, Object> config
) {
    public PipelineYamlQuery {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("query id must not be blank");
        }
        if (connector == null || connector.isBlank()) {
            throw new IllegalArgumentException("query connector must not be blank");
        }
        if (inputType == null || inputType.isBlank()) {
            throw new IllegalArgumentException("query inputType must not be blank");
        }
        if (outputType == null || outputType.isBlank()) {
            throw new IllegalArgumentException("query outputType must not be blank");
        }
        version = version == null || version.isBlank() ? "v1" : version;
        config = config == null ? Map.of() : Map.copyOf(config);
    }
}
