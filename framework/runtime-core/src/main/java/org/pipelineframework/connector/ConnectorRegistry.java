package org.pipelineframework.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Plain-Java provider catalog, validation boundary and deterministic lifecycle coordinator.
 */
public final class ConnectorRegistry {
    private final Map<ConnectorProviderId, ConnectorProvider<?>> providers;
    private final Map<ConnectorOperationIdentity, ConnectorOperation> operations;
    private final List<ConnectorProvider<?>> providerOrder;
    private final List<ConnectorProvider<?>> startedProviders = new ArrayList<>();
    private LifecycleState state = LifecycleState.NEW;
    private CompletionStage<Void> lifecycle = ConnectorCompletionStages.completed();

    public ConnectorRegistry(Collection<? extends ConnectorProvider<?>> discoveredProviders) {
        this(discoveredProviders, Set.of());
    }

    /**
     * Constructs a registry that explicitly admits the supplied framework-reserved provider IDs.
     * This is intended for framework host adapters; application and external providers should use
     * {@link #ConnectorRegistry(Collection)} instead.
     */
    public static ConnectorRegistry withFrameworkProviders(
        Collection<? extends ConnectorProvider<?>> discoveredProviders,
        Collection<ConnectorProviderId> frameworkProviderIds
    ) {
        return new ConnectorRegistry(discoveredProviders, Set.copyOf(
            Objects.requireNonNull(frameworkProviderIds, "framework provider IDs must not be null")));
    }

    private ConnectorRegistry(
        Collection<? extends ConnectorProvider<?>> discoveredProviders,
        Set<ConnectorProviderId> frameworkProviderIds
    ) {
        Objects.requireNonNull(discoveredProviders, "providers must not be null");
        for (ConnectorProviderId frameworkProviderId : frameworkProviderIds) {
            if (!Objects.requireNonNull(frameworkProviderId, "framework provider ID must not be null").isFrameworkReserved()) {
                throw new IllegalArgumentException(
                    "framework provider allowlist ID must use the reserved tpf namespace ('tpf' or 'tpf.*'): "
                        + frameworkProviderId.value());
            }
        }
        List<ConnectorProvider<?>> orderedProviders = new ArrayList<>();
        for (ConnectorProvider<?> provider : discoveredProviders) {
            orderedProviders.add(Objects.requireNonNull(provider, "provider must not be null"));
        }
        orderedProviders.sort(Comparator
            .comparing((ConnectorProvider<?> provider) -> provider.descriptor().id())
            .thenComparing(provider -> provider.getClass().getName()));
        providerOrder = List.copyOf(orderedProviders);
        Map<ConnectorProviderId, ConnectorProvider<?>> providersById = new LinkedHashMap<>();
        Map<ConnectorOperationIdentity, ConnectorOperation> operationsByIdentity = new LinkedHashMap<>();
        for (ConnectorProvider<?> provider : providerOrder) {
            ConnectorProviderDescriptor providerDescriptor = Objects.requireNonNull(
                provider.descriptor(), "provider descriptor must not be null");
            validateReservedProviderId(providerDescriptor.id(), frameworkProviderIds);
            validateProviderSchema(provider, providerDescriptor);
            if (providersById.putIfAbsent(providerDescriptor.id(), provider) != null) {
                throw new IllegalArgumentException("duplicate connector provider ID: " + providerDescriptor.id().value());
            }
            Collection<? extends ConnectorOperation> declaredOperations = Objects.requireNonNull(
                provider.operations(), "operations must not be null for provider " + providerDescriptor.id().value());
            for (ConnectorOperation operation : declaredOperations) {
                ConnectorOperation checkedOperation = Objects.requireNonNull(
                    operation, "operation must not be null for provider " + providerDescriptor.id().value());
                ConnectorOperationDescriptor operationDescriptor = Objects.requireNonNull(
                    checkedOperation.descriptor(), "operation descriptor must not be null for provider " + providerDescriptor.id().value());
                validateOperationFamily(checkedOperation, operationDescriptor, providerDescriptor);
                validateOperationSchema(checkedOperation, operationDescriptor, providerDescriptor);
                ConnectorOperationIdentity identity = ConnectorOperationIdentity.of(providerDescriptor, operationDescriptor);
                if (operationsByIdentity.putIfAbsent(identity, checkedOperation) != null) {
                    throw new IllegalArgumentException("duplicate connector operation identity: " + identity);
                }
            }
        }
        providers = Collections.unmodifiableMap(new LinkedHashMap<>(providersById));
        operations = Collections.unmodifiableMap(new LinkedHashMap<>(operationsByIdentity));
    }

    public static ConnectorRegistry discover(ClassLoader classLoader) {
        return new ConnectorRegistry(ConnectorProviderDiscovery.discover(classLoader));
    }

    public BoundConnectorRegistry bind(ConnectorProviderConfigurations configurations) {
        Objects.requireNonNull(configurations, "provider configurations must not be null");
        Map<ConnectorProviderId, Object> bound = new LinkedHashMap<>();
        for (ConnectorProvider<?> provider : providerOrder) {
            ConnectorProviderId id = provider.descriptor().id();
            ConnectorConfigurationDocument document = configurations.values().getOrDefault(id, ConnectorConfigurationDocument.empty());
            provider.configurationSchema().ifPresent(schema -> bound.put(id, bindProvider(schema, document, id)));
            if (provider.configurationSchema().isEmpty() && !document.values().isEmpty()) {
                throw new ConnectorConfigurationException("connector provider " + id.value() + " does not declare a configuration schema");
            }
        }
        configurations.values().keySet().stream()
            .filter(id -> !providers.containsKey(id))
            .sorted()
            .findFirst()
            .ifPresent(id -> {
                throw new ConnectorConfigurationException("configuration supplied for unknown connector provider: " + id.value());
            });
        return new BoundConnectorRegistry(this, bound);
    }

    public Map<ConnectorProviderId, ConnectorProvider<?>> providers() {
        return providers;
    }

    public Map<ConnectorOperationIdentity, ConnectorOperation> operations() {
        return operations;
    }

    public ConnectorProvider<?> requireProvider(ConnectorProviderId providerId, int expectedMajorVersion) {
        Objects.requireNonNull(providerId, "provider ID must not be null");
        ConnectorProvider<?> provider = providers.get(providerId);
        if (provider == null) {
            throw new IllegalStateException("no connector provider registered for ID: " + providerId.value());
        }
        int actualMajor = provider.descriptor().version().major();
        if (actualMajor != expectedMajorVersion) {
            throw new IllegalStateException(
                "incompatible connector provider major version for " + providerId.value() + ": requested "
                    + expectedMajorVersion + ", registered " + actualMajor + " (exact-major compatibility is required)");
        }
        return provider;
    }

    public ConnectorOperation requireOperation(ConnectorOperationIdentity identity) {
        Objects.requireNonNull(identity, "operation identity must not be null");
        ConnectorOperation operation = operations.get(identity);
        if (operation == null) {
            throw new IllegalStateException("no connector operation registered for identity: " + identity);
        }
        return operation;
    }

    public <T extends ConnectorOperation> T requireExecutionOperation(
        ConnectorOperationIdentity identity,
        Class<T> operationType
    ) {
        Objects.requireNonNull(operationType, "operation type must not be null");
        ConnectorOperation operation = requireOperation(identity);
        if (!isExecutionKindSupported(operation.descriptor().kind())) {
            throw new UnsupportedOperationException(
                "connector operation kind is registered as metadata only and has no execution path in M1: "
                    + operation.descriptor().kind().value());
        }
        if (!operationType.isInstance(operation)) {
            throw new IllegalStateException(
                "connector operation " + identity + " is " + operation.getClass().getName() + ", not " + operationType.getName());
        }
        return operationType.cast(operation);
    }

    public synchronized CompletionStage<Void> start(ConnectorRuntimeContext context) {
        Objects.requireNonNull(context, "runtime context must not be null");
        if (state == LifecycleState.NEW) {
            state = LifecycleState.STARTING;
            lifecycle = startSequentially(context)
                .whenComplete((ignored, failure) -> {
                    synchronized (this) {
                        if (state == LifecycleState.STARTING) {
                            state = failure == null ? LifecycleState.STARTED : LifecycleState.FAILED;
                        }
                    }
                });
        }
        if (state == LifecycleState.STOPPING || state == LifecycleState.STOPPED) {
            return failed("connector registry cannot start after stop has begun");
        }
        return lifecycle;
    }

    public synchronized CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        Objects.requireNonNull(context, "runtime context must not be null");
        if (state == LifecycleState.NEW) {
            state = LifecycleState.STOPPED;
            lifecycle = ConnectorCompletionStages.completed();
        } else if (state == LifecycleState.STARTING || state == LifecycleState.STARTED || state == LifecycleState.FAILED) {
            state = LifecycleState.STOPPING;
            lifecycle = lifecycle.handle((ignored, failure) -> ConnectorCompletionStages.completed())
                .thenCompose(ignored -> ignored)
                .thenCompose(ignored -> stopSequentially(context))
                .whenComplete((ignored, failure) -> {
                    synchronized (this) {
                        if (state == LifecycleState.STOPPING) {
                            state = failure == null ? LifecycleState.STOPPED : LifecycleState.FAILED;
                        }
                    }
                });
        }
        return lifecycle;
    }

    private static boolean isExecutionKindSupported(ConnectorOperationKind kind) {
        return ConnectorOperationKind.COMMAND.equals(kind) || ConnectorOperationKind.QUERY.equals(kind);
    }

    private static void validateReservedProviderId(
        ConnectorProviderId providerId,
        Set<ConnectorProviderId> frameworkProviderIds
    ) {
        if (providerId.isFrameworkReserved() && !frameworkProviderIds.contains(providerId)) {
            throw new IllegalArgumentException(
                "connector provider ID is reserved for framework use: " + providerId.value());
        }
    }

    List<ConnectorProvider<?>> providerOrder() {
        return providerOrder;
    }

    private static <PC> PC bindProvider(
        ConnectorConfigSchema<PC> schema,
        ConnectorConfigurationDocument document,
        ConnectorProviderId id
    ) {
        return ConnectorConfigurationBinder.bind(schema, document, "connector provider " + id.value());
    }

    private static void validateOperationFamily(
        ConnectorOperation operation,
        ConnectorOperationDescriptor descriptor,
        ConnectorProviderDescriptor provider
    ) {
        if (operation instanceof CommandOperation<?, ?, ?> && !ConnectorOperationKind.COMMAND.equals(descriptor.kind())) {
            throw new IllegalArgumentException(
                "command operation " + descriptor.id() + " for provider " + provider.id().value() + " must use kind "
                    + ConnectorOperationKind.COMMAND.value());
        }
        if (operation instanceof QueryOperation<?, ?, ?> && !ConnectorOperationKind.QUERY.equals(descriptor.kind())) {
            throw new IllegalArgumentException(
                "query operation " + descriptor.id() + " for provider " + provider.id().value() + " must use kind "
                    + ConnectorOperationKind.QUERY.value());
        }
        if (operation instanceof AgentOperation && !ConnectorOperationKind.AGENT.equals(descriptor.kind())) {
            throw new IllegalArgumentException(
                "agent operation " + descriptor.id() + " for provider " + provider.id().value() + " must use kind "
                    + ConnectorOperationKind.AGENT.value());
        }
    }

    private static void validateProviderSchema(ConnectorProvider<?> provider, ConnectorProviderDescriptor descriptor) {
        provider.configurationSchema().ifPresent(schema -> {
            if (!descriptor.configurationSchema().equals(java.util.Optional.of(schema.descriptor()))) {
                throw new IllegalArgumentException(
                    "connector provider " + descriptor.id().value() + " configuration schema does not match its descriptor");
            }
        });
    }

    private static void validateOperationSchema(
        ConnectorOperation operation,
        ConnectorOperationDescriptor descriptor,
        ConnectorProviderDescriptor provider
    ) {
        if (operation instanceof CommandOperation<?, ?, ?> command) {
            validateOperationSchema(command.configurationSchema(), descriptor, provider);
        }
        if (operation instanceof QueryOperation<?, ?, ?> query) {
            validateOperationSchema(query.configurationSchema(), descriptor, provider);
        }
    }

    private static void validateOperationSchema(
        java.util.Optional<? extends ConnectorConfigSchema<?>> schema,
        ConnectorOperationDescriptor descriptor,
        ConnectorProviderDescriptor provider
    ) {
        schema.ifPresent(value -> {
            if (!descriptor.configurationSchema().equals(java.util.Optional.of(value.descriptor()))) {
                throw new IllegalArgumentException(
                    "connector operation " + descriptor.id() + " for provider " + provider.id().value()
                        + " configuration schema does not match its descriptor");
            }
        });
    }

    private static List<ConnectorProvider<?>> reverse(List<ConnectorProvider<?>> providers) {
        List<ConnectorProvider<?>> reversed = new ArrayList<>(providers);
        java.util.Collections.reverse(reversed);
        return reversed;
    }

    private CompletionStage<Void> startSequentially(ConnectorRuntimeContext context) {
        CompletionStage<Void> sequence = ConnectorCompletionStages.completed();
        for (ConnectorProvider<?> provider : providerOrder) {
            sequence = sequence.thenCompose(ignored -> {
                CompletionStage<Void> stage = provider.start(context);
                if (stage == null) {
                    return failed("connector provider " + provider.descriptor().id().value() + " returned null from start");
                }
                return stage.thenRun(() -> recordStarted(provider));
            });
        }
        return sequence;
    }

    private synchronized void recordStarted(ConnectorProvider<?> provider) {
        startedProviders.add(provider);
    }

    private CompletionStage<Void> stopSequentially(ConnectorRuntimeContext context) {
        List<Throwable> failures = new ArrayList<>();
        CompletionStage<Void> sequence = ConnectorCompletionStages.completed();
        for (ConnectorProvider<?> provider : startedProvidersInReverseOrder()) {
            sequence = sequence.thenCompose(ignored -> stopProvider(context, provider, failures));
        }
        return sequence.thenCompose(ignored -> failures.isEmpty()
            ? ConnectorCompletionStages.completed()
            : failed(stopFailures(failures)));
    }

    private CompletionStage<Void> stopProvider(
        ConnectorRuntimeContext context,
        ConnectorProvider<?> provider,
        List<Throwable> failures
    ) {
        CompletionStage<Void> stage;
        try {
            stage = provider.stop(context);
        } catch (RuntimeException exception) {
            stage = CompletableFuture.failedFuture(exception);
        }
        if (stage == null) {
            stage = failed("connector provider " + provider.descriptor().id().value() + " returned null from stop");
        }
        return stage.handle((ignored, failure) -> {
            synchronized (this) {
                if (failure == null) {
                    startedProviders.remove(provider);
                } else {
                    failures.add(new IllegalStateException(
                        "connector provider " + provider.descriptor().id().value() + " failed to stop", failure));
                }
            }
            return null;
        });
    }

    private static IllegalStateException stopFailures(List<Throwable> failures) {
        IllegalStateException result = new IllegalStateException("one or more connector providers failed to stop");
        failures.forEach(result::addSuppressed);
        return result;
    }

    private synchronized List<ConnectorProvider<?>> startedProvidersInReverseOrder() {
        return reverse(startedProviders);
    }

    private static CompletionStage<Void> failed(String message) {
        return CompletableFuture.failedFuture(new IllegalStateException(message));
    }

    private static CompletionStage<Void> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }

    private enum LifecycleState {
        NEW,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
    }
}
