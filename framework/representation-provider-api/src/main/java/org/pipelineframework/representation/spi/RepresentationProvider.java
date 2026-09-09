package org.pipelineframework.representation.spi;

import java.util.List;
import java.util.Optional;

/**
 * Host-neutral representation lifecycle. Implementations describe intent and artifacts; a build host owns discovery,
 * ordering, diagnostics aggregation, and all file-system writes.
 */
public interface RepresentationProvider {
    ProviderMetadata metadata();

    default List<ProviderDiagnostic> validate(ProviderConfiguration configuration) {
        return List.of();
    }

    default Optional<ResolvedRepresentation> resolve(RepresentationMappingRequest mapping) {
        return Optional.empty();
    }

    default Optional<BoundaryClaim> claim(BoundaryRequest boundary) {
        return Optional.empty();
    }

    /** Declares whether this provider owns representation mapping for a Connector provider family. */
    default boolean supportsOperationProvider(String connectorProviderId, int connectorProviderMajorVersion) {
        return false;
    }

    default Optional<OperationBoundaryClaim> claimOperation(OperationBoundaryRequest boundary) {
        return Optional.empty();
    }

    default Optional<ResolvedOperationRepresentation> resolveOperation(OperationRepresentationRequest request) {
        return Optional.empty();
    }

    default List<ArtifactDescription> describeArtifacts(ProviderGenerationRequest request) {
        return List.of();
    }

    default List<ArtifactDescription> describeOperationArtifacts(OperationProviderGenerationRequest request) {
        return List.of();
    }

    default ProviderSchemaFragment schema() {
        return new ProviderSchemaFragment(metadata().key(), Optional.empty(), Optional.empty(), Optional.empty());
    }
}
