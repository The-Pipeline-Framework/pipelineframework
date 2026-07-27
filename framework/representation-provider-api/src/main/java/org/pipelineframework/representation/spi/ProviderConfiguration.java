package org.pipelineframework.representation.spi;

import java.util.LinkedHashMap;
import java.util.Map;

/** Opaque provider configuration at either GLOBAL or TYPE scope. */
public record ProviderConfiguration(RepresentationScope scope, String providerKey, Map<String, Object> options) {
    public ProviderConfiguration {
        if (scope == null) {
            throw new IllegalArgumentException("scope must not be null");
        }
        if (providerKey == null || providerKey.isBlank()) {
            throw new IllegalArgumentException("providerKey must not be blank");
        }
        providerKey = providerKey.trim();
        options = options == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(options));
    }
}
