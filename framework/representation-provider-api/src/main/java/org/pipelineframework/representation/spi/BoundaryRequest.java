package org.pipelineframework.representation.spi;

import java.util.Map;
import java.util.Set;

/** A target-neutral description of one configured boundary before core binds its service semantics. */
public record BoundaryRequest(
    String stepName,
    String serviceTypeName,
    CanonicalType inputType,
    CanonicalType outputType,
    String cardinality,
    Set<String> declaredBoundaryContracts,
    Map<String, Object> configuration
) {
    public BoundaryRequest {
        if (stepName == null || stepName.isBlank() || serviceTypeName == null || serviceTypeName.isBlank()) {
            throw new IllegalArgumentException("stepName and serviceTypeName must not be blank");
        }
        stepName = stepName.trim();
        serviceTypeName = serviceTypeName.trim();
        inputType = java.util.Objects.requireNonNull(inputType, "inputType must not be null");
        outputType = java.util.Objects.requireNonNull(outputType, "outputType must not be null");
        if (cardinality == null || cardinality.isBlank()) {
            throw new IllegalArgumentException("cardinality must not be blank");
        }
        cardinality = cardinality.trim();
        declaredBoundaryContracts = declaredBoundaryContracts == null ? Set.of() : Set.copyOf(declaredBoundaryContracts);
        configuration = ImmutableMapSupport.copy(configuration);
    }
}
