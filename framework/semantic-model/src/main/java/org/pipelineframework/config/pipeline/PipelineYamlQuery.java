package org.pipelineframework.config.pipeline;

/**
 * First-party captured query definition parsed from pipeline.yaml.
 */
public record PipelineYamlQuery(
    String id,
    String connector,
    String inputType,
    String outputType,
    String version,
    PipelineYamlJpaQuery jpa
) {
    public PipelineYamlQuery {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("query id must not be blank");
        }
        if (connector == null || connector.isBlank()) {
            throw new IllegalArgumentException("query connector must not be blank");
        }
        connector = connector.trim();
        if (!"jpa".equals(connector)) {
            throw new IllegalArgumentException("query connector supports only jpa in v1");
        }
        if (inputType == null || inputType.isBlank()) {
            throw new IllegalArgumentException("query inputType must not be blank");
        }
        if (outputType == null || outputType.isBlank()) {
            throw new IllegalArgumentException("query outputType must not be blank");
        }
        version = version == null || version.isBlank() ? "v1" : version;
        if (jpa == null) {
            throw new IllegalArgumentException("query jpa must be declared for connector jpa");
        }
    }
}
