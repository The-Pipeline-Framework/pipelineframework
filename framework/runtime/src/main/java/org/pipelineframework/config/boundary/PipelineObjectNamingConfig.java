package org.pipelineframework.config.boundary;

/**
 * Object publish key naming settings.
 *
 * @param keyTemplate object key template; supports {@code {groupKey}} and mapper labels
 */
public record PipelineObjectNamingConfig(String keyTemplate) {
    public PipelineObjectNamingConfig {
        keyTemplate = normalize(keyTemplate);
        if (keyTemplate == null) {
            keyTemplate = "{groupKey}";
        }
    }

    public static PipelineObjectNamingConfig defaults() {
        return new PipelineObjectNamingConfig("{groupKey}");
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
