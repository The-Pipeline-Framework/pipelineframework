package org.pipelineframework.config;

import java.util.Optional;

/**
 * Deployment platform mode for generated/runtime artifacts.
 */
public enum PlatformMode {
    STANDARD,
    LAMBDA;

    /**
     * Parses a platform mode from a string.
     *
     * @param raw input value
     * @return parsed platform mode or empty when unknown/blank
     */
    public static Optional<PlatformMode> fromStringOptional(String raw) {
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        String normalized = raw.trim();
        if ("STANDARD".equalsIgnoreCase(normalized)) {
            return Optional.of(STANDARD);
        }
        if ("LAMBDA".equalsIgnoreCase(normalized)) {
            return Optional.of(LAMBDA);
        }
        return Optional.empty();
    }
}

