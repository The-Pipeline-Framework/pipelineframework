package org.pipelineframework.config;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Shared platform override and normalization helpers.
 */
public final class PlatformOverrideResolver {
    public static final String PLATFORM_PROPERTY_KEY = "pipeline.platform";
    public static final String PLATFORM_ENV_KEY = "PIPELINE_PLATFORM";
    public static final Set<String> KNOWN_PLATFORMS = Stream.of(PlatformMode.values())
        .map(PlatformMode::name)
        .collect(Collectors.toUnmodifiableSet());

    private PlatformOverrideResolver() {
    }

    /**
     * Resolve platform override using properties first, then environment.
     *
     * <p>Looks up {@link #PLATFORM_PROPERTY_KEY} first and, if that result is null/blank, falls back to
     * {@link #PLATFORM_ENV_KEY}. Either lookup function may be null; null lookups are treated as providers
     * that always return null.</p>
     *
     * <p>This method may return {@code null} or a blank string if no override is configured. Callers should
     * normalize/validate the returned value (for example with {@link #normalizeKnownPlatform(String)})
     * before using it.</p>
     *
     * @param propertyLookup property lookup
     * @param envLookup environment lookup
     * @return resolved override value (possibly null or blank)
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
        return PlatformMode.fromStringOptional(rawPlatform).map(Enum::name).orElse(null);
    }
}
