package org.pipelineframework.processor.representation;

import java.util.List;
import java.util.Map;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.ResolvedRepresentation;

/** Host-owned normalized provider binding, resolved before core service classification. */
public record ResolvedProviderBoundary(
    BoundaryRequest boundary,
    BoundaryClaim claim,
    List<ResolvedRepresentation> representations,
    Map<String, Object> configuration
) {
    public ResolvedProviderBoundary {
        representations = List.copyOf(representations);
        configuration = Map.copyOf(configuration);
    }
}
