package org.pipelineframework.config.boundary;

import java.util.List;

/**
 * Object source include/exclude filter configuration.
 *
 * @param include include glob patterns
 * @param exclude exclude glob patterns
 */
public record PipelineObjectFilterConfig(
    List<String> include,
    List<String> exclude
) {
    public PipelineObjectFilterConfig {
        include = normalize(include);
        exclude = normalize(exclude);
    }

    public static PipelineObjectFilterConfig defaults() {
        return new PipelineObjectFilterConfig(List.of(), List.of());
    }

    private static List<String> normalize(List<String> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream()
            .filter(value -> value != null && !value.isBlank())
            .map(String::trim)
            .toList();
    }
}
