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
        for (PlatformMode mode : PlatformMode.values()) {
            if (mode.name().equalsIgnoreCase(normalized)) {
                return Optional.of(mode);
            }
        }
        return Optional.empty();
    }
}
