package org.pipelineframework.config.boundary;

import java.util.Optional;

/**
 * Object source admission binding for a pipeline input boundary.
 *
 * @param source named object source declared under top-level sources
 * @param type fully qualified domain input type emitted into the first pipeline step
 * @param typeName optional simple type name used by generator-facing templates
 * @param mapper mapper from one object snapshot to the emitted domain input type; absent for generated grouped selection
 * @param selection optional grouped object selection admitted as one typed input
 */
public record PipelineObjectInputConfig(
    String source,
    String type,
    String typeName,
    Optional<String> mapper,
    Optional<PipelineObjectSelectionConfig> selection
) {
    public PipelineObjectInputConfig(String source, String type, String typeName, String mapper) {
        this(source, type, typeName, Optional.ofNullable(mapper), Optional.empty());
    }

    public PipelineObjectInputConfig(String source, String type, String typeName, String mapper,
                                     Optional<PipelineObjectSelectionConfig> selection) {
        this(source, type, typeName, Optional.ofNullable(mapper), selection);
    }

    public PipelineObjectInputConfig {
        source = normalize(source);
        type = normalize(type);
        typeName = normalize(typeName);
        mapper = normalize(mapper);
        selection = selection == null ? Optional.empty() : selection;
        if (source == null) {
            throw new IllegalArgumentException("input.object.source must not be blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("input.object.emits.type must not be blank");
        }
        if (mapper.isEmpty() && selection.isEmpty()) {
            throw new IllegalArgumentException("input.object.emits.mapper must not be blank");
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static Optional<String> normalize(Optional<String> value) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String normalized = normalize(value.orElseThrow());
        return normalized == null ? Optional.empty() : Optional.of(normalized);
    }
}
