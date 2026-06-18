package org.pipelineframework.config.boundary;

import java.util.List;

/**
 * Object source deterministic identity settings.
 *
 * @param fields ordered object fields used for identity
 */
public record PipelineObjectIdentityConfig(
    List<String> fields
) {
    private static final List<String> DEFAULT_FIELDS = List.of("provider", "container", "key", "versionId", "etag");

    public PipelineObjectIdentityConfig {
        List<String> normalizedFields = fields == null
            ? List.of()
            : fields.stream()
                .filter(field -> field != null && !field.isBlank())
                .map(String::trim)
                .toList();
        fields = normalizedFields.isEmpty() ? DEFAULT_FIELDS : normalizedFields;
    }

    public static PipelineObjectIdentityConfig defaults() {
        return new PipelineObjectIdentityConfig(DEFAULT_FIELDS);
    }
}
