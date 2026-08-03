package org.pipelineframework.representation.spi;

import java.util.Set;

/** Provider identity and ordering declaration. Library dependencies are deliberately not ordering metadata. */
public record ProviderMetadata(String key, Set<String> requiresProviders, Set<String> capabilities) {
    public ProviderMetadata {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("provider key must not be blank");
        }
        key = key.trim();
        requiresProviders = requiresProviders == null ? Set.of() : Set.copyOf(requiresProviders);
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
    }
}
