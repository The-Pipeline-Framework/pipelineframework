package org.pipelineframework.representation.spi;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** A normalized explicit representation mapping; options remain provider-owned and opaque to core. */
public record RepresentationMappingRequest(
    String key,
    CanonicalType domainType,
    Optional<String> representationType,
    Optional<String> mapperType,
    Map<String, Object> options
) {
    public RepresentationMappingRequest {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        key = key.trim();
        domainType = Objects.requireNonNull(domainType, "domainType must not be null");
        representationType = normalized(representationType);
        mapperType = normalized(mapperType);
        options = ImmutableMapSupport.copy(options);
    }

    private static Optional<String> normalized(Optional<String> value) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String candidate = value.get().trim();
        if (candidate.isEmpty()) {
            throw new IllegalArgumentException("representation mapping class name must not be blank");
        }
        return Optional.of(candidate);
    }
}
