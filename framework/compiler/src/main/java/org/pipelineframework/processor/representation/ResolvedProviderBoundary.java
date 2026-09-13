package org.pipelineframework.processor.representation;

import java.util.List;
import java.util.Map;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.ImmutableMapSupport;
import org.pipelineframework.representation.spi.ResolvedRepresentation;
import org.pipelineframework.processor.composition.PipelineReference;

/** Host-owned normalized provider binding, resolved before core service classification. */
public record ResolvedProviderBoundary(
    PipelineReference definition,
    BoundaryRequest boundary,
    BoundaryClaim claim,
    List<ResolvedRepresentation> representations,
    Map<String, Object> configuration
) {
    public ResolvedProviderBoundary {
        definition = java.util.Objects.requireNonNull(definition, "definition must not be null");
        representations = List.copyOf(representations);
        configuration = ImmutableMapSupport.copy(configuration);
    }

    public ResolvedProviderBoundary(
        BoundaryRequest boundary,
        BoundaryClaim claim,
        List<ResolvedRepresentation> representations,
        Map<String, Object> configuration
    ) {
        this(new PipelineReference("$root"), boundary, claim, representations, configuration);
    }
}
