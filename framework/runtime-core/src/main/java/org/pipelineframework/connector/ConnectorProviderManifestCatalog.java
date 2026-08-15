package org.pipelineframework.connector;

import java.util.*;

import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/**
 * Deterministically validated static provider metadata catalog for compiler/build-time discovery.
 */
public final class ConnectorProviderManifestCatalog {
    private final Map<ConnectorProviderId, ConnectorProviderArtifactDescriptor> providers;
    private final Map<ConnectorOperationIdentity, ConnectorOperationDescriptor> operations;
    private final Map<ProtocolTypeIdentity, ProtocolTypeDescriptor> protocolTypes;

    public ConnectorProviderManifestCatalog(Collection<ConnectorProviderManifest> manifests) {
        Objects.requireNonNull(manifests, "manifests must not be null");
        List<ConnectorProviderArtifactDescriptor> descriptors = manifests.stream()
            .flatMap(manifest -> Objects.requireNonNull(manifest, "manifest must not be null").providers().stream())
            .sorted(Comparator.comparing(descriptor -> descriptor.provider().id()))
            .toList();
        Map<ConnectorProviderId, ConnectorProviderArtifactDescriptor> providersById = new LinkedHashMap<>();
        Map<ConnectorOperationIdentity, ConnectorOperationDescriptor> operationsById = new LinkedHashMap<>();
        Map<ProtocolTypeIdentity, ProtocolTypeDescriptor> protocolTypesById = new LinkedHashMap<>();
        for (ConnectorProviderArtifactDescriptor descriptor : descriptors) {
            ConnectorProviderArtifactDescriptor duplicate = providersById.putIfAbsent(descriptor.provider().id(), descriptor);
            if (duplicate != null) {
                throw new IllegalArgumentException("duplicate connector provider ID in static metadata: " + descriptor.provider().id().value());
            }
            for (ConnectorOperationDescriptor operation : descriptor.operations()) {
                ConnectorOperationIdentity identity = ConnectorOperationIdentity.of(descriptor.provider(), operation);
                if (operationsById.putIfAbsent(identity, operation) != null) {
                    throw new IllegalArgumentException("duplicate connector operation identity in static metadata: " + identity);
                }
            }
            for (ProtocolTypeDescriptor protocolType : descriptor.protocolTypes()) {
                if (protocolTypesById.putIfAbsent(protocolType.identity(), protocolType) != null) {
                    throw new IllegalArgumentException(
                        "duplicate protocol type identity in static metadata: " + protocolType.identity());
                }
            }
        }
        providers = Collections.unmodifiableMap(new LinkedHashMap<>(providersById));
        operations = Collections.unmodifiableMap(new LinkedHashMap<>(operationsById));
        protocolTypes = Collections.unmodifiableMap(new LinkedHashMap<>(protocolTypesById));
    }

    public List<ConnectorProviderArtifactDescriptor> providers() {
        return List.copyOf(providers.values());
    }

    public Map<ConnectorOperationIdentity, ConnectorOperationDescriptor> operations() {
        return operations;
    }

    public Map<ProtocolTypeIdentity, ProtocolTypeDescriptor> protocolTypes() {
        return protocolTypes;
    }

    public void validateCommandPolicy(
        ConnectorOperationIdentity identity,
        int expectedProviderMajorVersion,
        CommandPolicy policy
    ) {
        Objects.requireNonNull(identity, "command operation identity must not be null");
        ConnectorProviderArtifactDescriptor provider = providers.get(identity.providerId());
        if (provider == null) {
            throw new IllegalArgumentException("no connector provider static metadata found for ID: " + identity.providerId().value());
        }
        if (provider.provider().version().major() != expectedProviderMajorVersion) {
            throw new IllegalArgumentException("incompatible connector provider major version in static metadata for "
                + identity.providerId().value() + ": requested " + expectedProviderMajorVersion + ", published "
                + provider.provider().version().major());
        }
        ConnectorOperationDescriptor operation = operations.get(identity);
        if (operation == null) {
            throw new IllegalArgumentException("no connector command operation static metadata found for identity: " + identity);
        }
        CommandPolicyValidator.validate(provider.provider(), operation, policy);
    }
}
