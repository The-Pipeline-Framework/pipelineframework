package org.pipelineframework.config;

import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * Shared platform override and normalization helpers.
 */
public final class PlatformOverrideResolver {
    public static final String PLATFORM_PROPERTY_KEY = "pipeline.platform";
    public static final String PLATFORM_ENV_KEY = "PIPELINE_PLATFORM";
    public static final Set<String> KNOWN_PLATFORMS = Set.of("STANDARD", "LAMBDA");

    private PlatformOverrideResolver() {
    }

    /**
     * Resolve platform override using properties first, then environment.
     *
     * @param propertyLookup property lookup
     * @param envLookup environment lookup
     * @return resolved override value
     */
    public static String resolveOverride(Function<String, String> propertyLookup, Function<String, String> envLookup) {
        Function<String, String> properties = propertyLookup == null ? key -> null : propertyLookup;
        Function<String, String> env = envLookup == null ? key -> null : envLookup;
        String override = properties.apply(PLATFORM_PROPERTY_KEY);
        if (override == null || override.isBlank()) {
            override = env.apply(PLATFORM_ENV_KEY);
        }
        return override;
    }

    /**
     * Normalize and validate platform identifier.
     *
     * @param rawPlatform raw input
     * @return normalized platform or null when unknown
     */
    public static String normalizeKnownPlatform(String rawPlatform) {
        if (rawPlatform == null) {
            return null;
        }
        String trimmed = rawPlatform.trim();
        if (trimmed.isBlank()) {
            return null;
        }
        String normalized = trimmed.toUpperCase(Locale.ROOT);
        return KNOWN_PLATFORMS.contains(normalized) ? normalized : null;
    }
}

