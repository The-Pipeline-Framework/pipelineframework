package org.pipelineframework.representation.spi;

import java.util.Objects;
import java.util.Optional;

/** One canonical-to-wire mapping request after Connector operation selection. */
public record OperationRepresentationRequest(
    OperationBoundaryRequest boundary,
    OperationBoundaryClaim claim,
    OperationRepresentationRole role,
    CanonicalType canonicalType,
    String canonicalSchemaJson,
    OperationBoundaryClaim.WireBoundary wireBoundary,
    Optional<RepresentationMappingRequest> authoredMapping
) {
    public OperationRepresentationRequest {
        boundary = Objects.requireNonNull(boundary, "operation boundary must not be null");
        claim = Objects.requireNonNull(claim, "operation boundary claim must not be null");
        role = Objects.requireNonNull(role, "operation representation role must not be null");
        canonicalType = Objects.requireNonNull(canonicalType, "operation canonical type must not be null");
        canonicalSchemaJson = Objects.requireNonNull(canonicalSchemaJson,
            "operation canonical schema must not be null").trim();
        if (canonicalSchemaJson.isEmpty()) throw new IllegalArgumentException("operation canonical schema must not be blank");
        wireBoundary = Objects.requireNonNull(wireBoundary, "operation wire boundary must not be null");
        authoredMapping = Objects.requireNonNull(authoredMapping, "authored operation mapping must not be null");
        if (authoredMapping.isPresent()) {
            RepresentationMappingRequest mapping = authoredMapping.orElseThrow();
            if (!wireBoundary.mappingKey().equals(mapping.key())) {
                throw new IllegalArgumentException("authored operation mapping key disagrees with provider claim");
            }
            if (!canonicalType.equals(mapping.domainType())) {
                throw new IllegalArgumentException("authored operation mapping canonical type disagrees with boundary");
            }
        }
    }
}
