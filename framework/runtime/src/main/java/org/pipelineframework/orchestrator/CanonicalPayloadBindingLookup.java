package org.pipelineframework.orchestrator;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Resolves a release-contract type reference to its named canonical definition. */
public final class CanonicalPayloadBindingLookup {
    private CanonicalPayloadBindingLookup() {
    }

    /**
     * A step contract may carry the generated Java type while the canonical catalog is keyed by
     * the named canonical definition. The runtime-class match preserves the catalog's canonical
     * identity instead of treating a Java name as a second durable type identity.
     */
    public static Optional<ResolvedCanonicalDefinition> resolve(
        Map<String, Map<String, Object>> definitions, String requestedTypeId) {
        Objects.requireNonNull(definitions, "definitions");
        Objects.requireNonNull(requestedTypeId, "requestedTypeId");

        Map<String, Object> direct = definitions.get(requestedTypeId);
        if (direct != null) {
            return Optional.of(new ResolvedCanonicalDefinition(requestedTypeId, direct));
        }
        return definitions.entrySet().stream()
            .filter(entry -> requestedTypeId.equals(entry.getValue().get("runtimeClass")))
            .map(entry -> new ResolvedCanonicalDefinition(entry.getKey(), entry.getValue()))
            .findFirst();
    }

    public record ResolvedCanonicalDefinition(String canonicalTypeId, Map<String, Object> definition) {
        public ResolvedCanonicalDefinition {
            canonicalTypeId = Objects.requireNonNull(canonicalTypeId, "canonicalTypeId");
            definition = Map.copyOf(Objects.requireNonNull(definition, "definition"));
        }
    }
}
