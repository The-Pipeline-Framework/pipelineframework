package org.pipelineframework.config.template;

import java.util.Locale;
import java.util.Optional;

/**
 * Supported deployment platforms in pipeline template configuration.
 */
public enum PipelinePlatform {
    COMPUTE,
    FUNCTION;

    /**
     * Parse a pipeline platform from canonical or legacy aliases.
     *
     * @param raw raw value
     * @return parsed platform or empty when unknown
     */
    public static Optional<PipelinePlatform> fromStringOptional(String raw) {
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "STANDARD", "COMPUTE" -> Optional.of(COMPUTE);
            case "LAMBDA", "FUNCTION" -> Optional.of(FUNCTION);
            default -> Optional.empty();
        };
    }
}
