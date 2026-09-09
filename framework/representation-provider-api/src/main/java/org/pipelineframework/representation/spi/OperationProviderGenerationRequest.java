package org.pipelineframework.representation.spi;

import java.util.List;
import java.util.Objects;

/** Complete deterministic operation-representation set supplied to one provider. */
public record OperationProviderGenerationRequest(List<ResolvedOperationRepresentation> representations) {
    public OperationProviderGenerationRequest {
        representations = List.copyOf(Objects.requireNonNull(representations,
            "resolved operation representations must not be null"));
        representations.forEach(value -> Objects.requireNonNull(value,
            "resolved operation representation must not be null"));
    }
}
