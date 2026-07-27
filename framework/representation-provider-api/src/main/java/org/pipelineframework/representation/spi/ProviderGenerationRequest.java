package org.pipelineframework.representation.spi;

import java.util.List;
import java.util.Map;

/** Generation input supplied by a host after it has resolved provider ownership. */
public record ProviderGenerationRequest(BoundaryRequest boundary, BoundaryClaim claim,
                                        List<ResolvedRepresentation> representations,
                                        Map<String, Object> globalConfiguration) {
    public ProviderGenerationRequest {
        boundary = java.util.Objects.requireNonNull(boundary, "boundary must not be null");
        claim = java.util.Objects.requireNonNull(claim, "claim must not be null");
        representations = representations == null ? List.of() : List.copyOf(representations);
        globalConfiguration = globalConfiguration == null ? Map.of() : Map.copyOf(globalConfiguration);
    }
}
