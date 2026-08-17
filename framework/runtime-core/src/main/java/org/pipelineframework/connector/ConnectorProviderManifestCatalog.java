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
        return List.copyOf(providers.values());
    }

    public Map<ConnectorOperationIdentity, ConnectorOperationDescriptor> operations() {
        return operations;
    }

    public ConnectorProviderArtifactDescriptor requireProvider(
        ConnectorProviderId providerId,
        int expectedProviderMajorVersion
    ) {
        Objects.requireNonNull(providerId, "connector provider ID must not be null");
        ConnectorProviderArtifactDescriptor provider = providers.get(providerId);
        if (provider == null) {
            throw new IllegalArgumentException("no connector provider static metadata found for ID: " + providerId.value());
        }
        if (provider.provider().version().major() != expectedProviderMajorVersion) {
            throw new IllegalArgumentException("incompatible connector provider major version in static metadata for "
                + providerId.value() + ": requested " + expectedProviderMajorVersion + ", published "
                + provider.provider().version().major());
        }
        return provider;
    }

    public ConnectorOperationDescriptor requireOperation(
        ConnectorProviderId providerId,
        int expectedProviderMajorVersion,
        String operationId,
        ConnectorOperationKind kind,
        int operationMajorVersion
    ) {
        requireProvider(providerId, expectedProviderMajorVersion);
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            providerId, operationId, kind, operationMajorVersion);
        ConnectorOperationDescriptor operation = operations.get(identity);
        if (operation == null) {
            throw new IllegalArgumentException("no connector " + kind.value()
                + " operation static metadata found for identity: " + identity);
        }
        return operation;
    }

    public void validateProviderConfiguration(
        ConnectorProviderId providerId,
        int expectedProviderMajorVersion,
        ConnectorConfigurationDocument configuration,
        String subject
    ) {
        ConnectorProviderDescriptor provider = requireProvider(providerId, expectedProviderMajorVersion).provider();
        validateConfiguration(provider.configurationSchema(), configuration, subject);
    }

    public void validateOperationConfiguration(
        ConnectorProviderId providerId,
        int expectedProviderMajorVersion,
        String operationId,
        ConnectorOperationKind kind,
        int operationMajorVersion,
        ConnectorConfigurationDocument configuration,
        String subject
    ) {
        ConnectorOperationDescriptor operation = requireOperation(
            providerId, expectedProviderMajorVersion, operationId, kind, operationMajorVersion);
        validateConfiguration(operation.configurationSchema(), configuration, subject);
    }

    public void validateCommandPolicy(
        ConnectorOperationIdentity identity,
        int expectedProviderMajorVersion,
        CommandPolicy policy
    ) {
        Objects.requireNonNull(identity, "command operation identity must not be null");
        ConnectorProviderArtifactDescriptor provider = requireProvider(identity.providerId(), expectedProviderMajorVersion);
        ConnectorOperationDescriptor operation = operations.get(identity);
        if (operation == null) {
            throw new IllegalArgumentException("no connector command operation static metadata found for identity: " + identity);
        }
        CommandPolicyValidator.validate(provider.provider(), operation, policy);
    }

    public QueryCapabilities requireQueryCapabilities(
        ConnectorOperationIdentity identity,
        int expectedProviderMajorVersion
    ) {
        Objects.requireNonNull(identity, "query operation identity must not be null");
        if (identity.kind() != ConnectorOperationKind.QUERY) {
            throw new IllegalArgumentException("query capabilities require a Query operation identity: " + identity);
        }
        ConnectorOperationDescriptor operation = requireOperation(
            identity.providerId(),
            expectedProviderMajorVersion,
            identity.operationId(),
            ConnectorOperationKind.QUERY,
            identity.majorVersion());
        return operation.queryCapabilities().orElse(QueryCapabilities.conservative());
    }

    private static void validateConfiguration(
        java.util.Optional<ConnectorConfigSchemaDescriptor> schema,
        ConnectorConfigurationDocument configuration,
        String subject
    ) {
        Objects.requireNonNull(configuration, "connector configuration must not be null");
        Objects.requireNonNull(subject, "connector configuration subject must not be null");
        if (schema.isEmpty()) {
            if (!configuration.values().isEmpty()) {
                throw new ConnectorConfigurationException(subject + " does not declare a configuration schema");
            }
            return;
        }
        ConnectorConfigurationValidator.validate(schema.orElseThrow(), configuration, subject);
    }
}
