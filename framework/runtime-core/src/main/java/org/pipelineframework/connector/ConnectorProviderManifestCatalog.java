package org.pipelineframework.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministically validated static provider metadata catalog for compiler/build-time discovery.
 */
public final class ConnectorProviderManifestCatalog {
    private final Map<ConnectorProviderId, ConnectorProviderArtifactDescriptor> providers;
    private final Map<ConnectorOperationIdentity, ConnectorOperationDescriptor> operations;

    public ConnectorProviderManifestCatalog(Collection<ConnectorProviderManifest> manifests) {
        Objects.requireNonNull(manifests, "manifests must not be null");
        List<ConnectorProviderArtifactDescriptor> descriptors = manifests.stream()
            .flatMap(manifest -> Objects.requireNonNull(manifest, "manifest must not be null").providers().stream())
            .sorted(Comparator.comparing(descriptor -> descriptor.provider().id()))
            .toList();
        Map<ConnectorProviderId, ConnectorProviderArtifactDescriptor> providersById = new LinkedHashMap<>();
        Map<ConnectorOperationIdentity, ConnectorOperationDescriptor> operationsById = new LinkedHashMap<>();
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
        }
        providers = Collections.unmodifiableMap(new LinkedHashMap<>(providersById));
        operations = Collections.unmodifiableMap(new LinkedHashMap<>(operationsById));
    }

    public List<ConnectorProviderArtifactDescriptor> providers() {
        return new ArrayList<>(providers.values());
    }

    public Map<ConnectorOperationIdentity, ConnectorOperationDescriptor> operations() {
        return operations;
    }
}
