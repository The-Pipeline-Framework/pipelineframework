package org.pipelineframework.config;

import java.util.Locale;
import java.util.Optional;

/**
 * Deployment platform mode for generated/runtime artifacts.
 */
public enum PlatformMode {
    COMPUTE,
    FUNCTION;

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
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        for (PlatformMode mode : values()) {
            if (mode.name().equals(normalized)
                || mode.legacyExternalName().equals(normalized)) {
                return Optional.of(mode);
            }
        }
        return Optional.empty();
    }

    /**
     * Indicates whether this mode represents function execution (for example, AWS Lambda).
     *
     * @return true when mode is FUNCTION
     */
    public boolean isFunction() {
        return this == FUNCTION;
    }

    /**
     * Indicates whether this mode represents traditional compute deployment.
     *
     * @return true when mode is COMPUTE
     */
    public boolean isCompute() {
        return this == COMPUTE;
    }

    /**
     * Backward-compatible alias for FUNCTION checks.
     *
     * @return true when mode is FUNCTION
     */
    @Deprecated(forRemoval = false)
    public boolean isLambda() {
        return isFunction();
    }

    /**
     * Backward-compatible alias for COMPUTE checks.
     *
     * @return true when mode is COMPUTE
     */
    @Deprecated(forRemoval = false)
    public boolean isStandard() {
        return isCompute();
    }

    /**
     * Canonical external property value for this mode.
     *
     * @return COMPUTE or FUNCTION
     */
    public String externalName() {
        return name();
    }

    /**
     * Backward-compatible external alias for this mode.
     *
     * @return STANDARD for COMPUTE, LAMBDA for FUNCTION
     */
    public String legacyExternalName() {
        return this == FUNCTION ? "LAMBDA" : "STANDARD";
    }
}
