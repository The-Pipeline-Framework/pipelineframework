package org.pipelineframework.config.boundary;

/**
 * Object source admission binding for a pipeline input boundary.
 *
 * @param source named object source declared under top-level sources
 * @param type fully qualified domain input type emitted into the first pipeline step
 * @param typeName optional simple type name used by generator-facing templates
 * @param mapper mapper from object snapshot to the emitted domain input type
 */
public record PipelineObjectInputConfig(
    String source,
    String type,
    String typeName,
    String mapper
) {
    public PipelineObjectInputConfig {
        source = normalize(source);
        type = normalize(type);
        typeName = normalize(typeName);
        mapper = normalize(mapper);
        if (source == null) {
            throw new IllegalArgumentException("input.object.source must not be blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("input.object.emits.type must not be blank");
        }
        if (mapper == null) {
            throw new IllegalArgumentException("input.object.emits.mapper must not be blank");
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
