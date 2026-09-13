package org.pipelineframework.config.boundary;

/**
 * Object publish binding for a pipeline output boundary.
 *
 * @param target named publish target declared under top-level publish
 * @param type fully qualified terminal output type consumed by the publish mapper
 * @param typeName optional simple type name used by generator-facing templates
 * @param mapper mapper from terminal output items to grouped object payloads
 */
public record PipelineObjectOutputConfig(
    String target,
    String type,
    String typeName,
    String mapper
) {
    public PipelineObjectOutputConfig {
        target = normalize(target);
        type = normalize(type);
        typeName = normalize(typeName);
        mapper = normalize(mapper);
        if (target == null) {
            throw new IllegalArgumentException("output.object.target must not be blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("output.object.consumes.type must not be blank");
        }
        if (mapper == null) {
            throw new IllegalArgumentException("output.object.consumes.mapper must not be blank");
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
