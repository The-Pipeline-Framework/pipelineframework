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
        representationType = representationType == null ? Optional.empty() : representationType;
        mapperType = mapperType == null ? Optional.empty() : mapperType;
    }
}
