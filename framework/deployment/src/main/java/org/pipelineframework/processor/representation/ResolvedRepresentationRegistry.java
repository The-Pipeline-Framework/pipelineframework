package org.pipelineframework.processor.representation;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.pipelineframework.representation.spi.ResolvedRepresentation;

/** Host-owned registry available to component consumers after provider resolution. */
public final class ResolvedRepresentationRegistry {
    private final Map<String, ResolvedRepresentation> representations = new LinkedHashMap<>();

    public void register(ResolvedRepresentation representation) {
        String key = representation.domainType().name() + "#" + representation.providerKey();
        if (representations.putIfAbsent(key, representation) != null) {
            throw new IllegalStateException("Duplicate resolved representation '" + key + "'.");
        }
    }

    public Optional<ResolvedRepresentation> find(String canonicalType, String providerKey) {
        return Optional.ofNullable(representations.get(canonicalType + "#" + providerKey));
    }

    public Map<String, ResolvedRepresentation> all() {
        return Map.copyOf(representations);
    }
}
