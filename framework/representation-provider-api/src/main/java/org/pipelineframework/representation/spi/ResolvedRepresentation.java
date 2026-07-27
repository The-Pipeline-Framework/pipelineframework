package org.pipelineframework.representation.spi;

import java.util.Optional;

/** Resolved mapping information a consumer may use without reimplementing provider selection. */
public record ResolvedRepresentation(
    String providerKey,
    CanonicalType domainType,
    Optional<String> representationType,
    Optional<String> mapperType
) {
    public ResolvedRepresentation {
        if (providerKey == null || providerKey.isBlank() || domainType == null) {
            throw new IllegalArgumentException("providerKey and domainType must be present");
        }
        providerKey = providerKey.trim();
        representationType = normalized(representationType, "representationType");
        mapperType = normalized(mapperType, "mapperType");
    }

    private static Optional<String> normalized(Optional<String> value, String fieldName) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String normalized = value.orElseThrow().trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank when present");
        }
        return Optional.of(normalized);
    }
}
