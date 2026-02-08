package org.pipelineframework.config;

import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * Shared transport override and normalization helpers.
 */
public final class TransportOverrideResolver {
    public static final String TRANSPORT_PROPERTY_KEY = "pipeline.transport";
    public static final String TRANSPORT_ENV_KEY = "PIPELINE_TRANSPORT";
    public static final Set<String> KNOWN_TRANSPORTS = Set.of("GRPC", "REST", "LOCAL");

    /**
     * Prevents instantiation of this utility class.
     */
    private TransportOverrideResolver() {
    }

    /**
     * Determine the transport override by consulting properties first and then environment variables.
     *
     * @param propertyLookup function that returns a property value for a given key; the method will call it with {@code TRANSPORT_PROPERTY_KEY}. If {@code null}, a lookup that returns {@code null} is assumed.
     * @param envLookup      function that returns an environment value for a given key; the method will call it with {@code TRANSPORT_ENV_KEY}. If {@code null}, a lookup that returns {@code null} is assumed.
     * @return               the transport override found: the property value if present and not blank, otherwise the environment value; may be {@code null} or blank if none is set
     */
    public static String resolveOverride(Function<String, String> propertyLookup, Function<String, String> envLookup) {
        Function<String, String> properties = propertyLookup == null ? key -> null : propertyLookup;
        Function<String, String> env = envLookup == null ? key -> null : envLookup;
        String override = properties.apply(TRANSPORT_PROPERTY_KEY);
        if (override == null || override.isBlank()) {
            override = env.apply(TRANSPORT_ENV_KEY);
        }
        return override;
    }

    /**
     * Normalize and validate a transport identifier against the set of known transports.
     *
     * Trims the input, converts it to upper case using Locale.ROOT, and returns the normalized value if it matches a known transport; returns null for null, blank, or unrecognized inputs.
     *
     * @param rawTransport the raw transport string to normalize
     * @return the normalized transport identifier (for example "GRPC", "REST", or "LOCAL") if recognized, or null otherwise
     */
    public static String normalizeKnownTransport(String rawTransport) {
        if (rawTransport == null) {
            return null;
        }
        String trimmed = rawTransport.trim();
        if (trimmed.isBlank()) {
            return null;
        }
        String normalized = trimmed.toUpperCase(Locale.ROOT);
        return KNOWN_TRANSPORTS.contains(normalized) ? normalized : null;
    }
}