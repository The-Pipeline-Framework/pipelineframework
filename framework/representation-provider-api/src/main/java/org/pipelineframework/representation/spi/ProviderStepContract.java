package org.pipelineframework.representation.spi;

/** Provider-declared canonical step contract for a generated facade. */
public record ProviderStepContract(ProviderExecutionStyle executionStyle, String cardinality) {
    public ProviderStepContract {
        if (executionStyle == null || cardinality == null || cardinality.isBlank()) {
            throw new IllegalArgumentException("provider step contract requires execution style and cardinality");
        }
        cardinality = cardinality.trim();
    }
}
