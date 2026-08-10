package org.pipelineframework.representation.spi;

import java.util.Set;

/** Provider-declared canonical step contract for a generated facade. */
public record ProviderStepContract(ProviderExecutionStyle executionStyle, String cardinality,
                                   Set<ProviderCapability> capabilities) {
    private static final Set<String> SUPPORTED_CARDINALITIES = Set.of(
        "UNARY_UNARY", "UNARY_STREAMING", "STREAMING_UNARY", "STREAMING_STREAMING");

    public ProviderStepContract {
        if (executionStyle == null || cardinality == null || cardinality.isBlank()) {
            throw new IllegalArgumentException("provider step contract requires execution style and cardinality");
        }
        cardinality = cardinality.trim();
        if (!SUPPORTED_CARDINALITIES.contains(cardinality)) {
            throw new IllegalArgumentException("Unsupported provider step cardinality: " + cardinality);
        }
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
    }

    public ProviderStepContract(ProviderExecutionStyle executionStyle, String cardinality) {
        this(executionStyle, cardinality, Set.of());
    }
}
