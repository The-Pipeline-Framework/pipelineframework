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

    private TransportOverrideResolver() {
    }

    public static String resolveOverride(Function<String, String> propertyLookup, Function<String, String> envLookup) {
        Function<String, String> properties = propertyLookup == null ? key -> null : propertyLookup;
        Function<String, String> env = envLookup == null ? key -> null : envLookup;
        String override = properties.apply(TRANSPORT_PROPERTY_KEY);
        if (override == null || override.isBlank()) {
            override = env.apply(TRANSPORT_ENV_KEY);
        }
        return override;
    }

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
