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
    private final Map<ConnectorProviderId, CompletionStage<Void>> activations = new LinkedHashMap<>();
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
            .comparing((ConnectorProvider<?> provider) -> provider.id())
            .thenComparing(provider -> provider.getClass().getName()));
        providerOrder = List.copyOf(orderedProviders);
        Map<ConnectorProviderId, ConnectorProvider<?>> providersById = new LinkedHashMap<>();
        Map<ConnectorOperationIdentity, ConnectorOperation> operationsByIdentity = new LinkedHashMap<>();
        for (ConnectorProvider<?> provider : providerOrder) {
            ConnectorProviderDescriptor providerDescriptor = ConnectorDescriptors.provider(provider);
            validateReservedProviderId(providerDescriptor.id(), frameworkProviderIds);
            if (providersById.putIfAbsent(providerDescriptor.id(), provider) != null) {
                throw new IllegalArgumentException("duplicate connector provider ID: " + providerDescriptor.id().value());
            }
            Collection<? extends ConnectorOperation> declaredOperations = Objects.requireNonNull(
                provider.operations(), "operations must not be null for provider " + providerDescriptor.id().value());
            for (ConnectorOperation operation : declaredOperations) {
                ConnectorOperation checkedOperation = Objects.requireNonNull(
                    operation, "operation must not be null for provider " + providerDescriptor.id().value());
                ConnectorOperationDescriptor operationDescriptor = ConnectorDescriptors.operation(checkedOperation);
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
            ConnectorProviderId id = provider.id();
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
        int actualMajor = provider.version().major();
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
        if (!operationType.isInstance(operation)) {
            throw new IllegalStateException(
                "connector operation " + identity + " is " + operation.getClass().getName() + ", not " + operationType.getName());
        }
        return operationType.cast(operation);
    }

    public CommandOperation<?, ?, ?> requireCommandOperation(
        ConnectorOperationIdentity identity,
        int expectedProviderMajorVersion,
        CommandPolicy policy
    ) {
        Objects.requireNonNull(identity, "command operation identity must not be null");
        requireProvider(identity.providerId(), expectedProviderMajorVersion);
        CommandOperation<?, ?, ?> operation = requireExecutionOperation(identity, CommandOperation.class);
        CommandPolicyValidator.validate(
            ConnectorDescriptors.provider(providers.get(identity.providerId())), ConnectorDescriptors.operation(operation), policy);
        return operation;
    }

    public synchronized CompletionStage<Void> start(ConnectorRuntimeContext context) {
        Objects.requireNonNull(context, "runtime context must not be null");
        if (state == LifecycleState.STOPPING || state == LifecycleState.STOPPED) {
            return failed("connector registry cannot start after stop has begun");
        }
        state = LifecycleState.RUNNING;
        CompletionStage<Void> sequence = ConnectorCompletionStages.completed();
        for (ConnectorProvider<?> provider : providerOrder) {
            sequence = sequence.thenCompose(ignored -> activate(provider.id(), context));
        }
        lifecycle = sequence;
        return lifecycle;
    }

    /**
     * Lazily activates one discovered provider for deprecated provider-first live execution.
     * Discovery alone never makes the provider a lifecycle or resource owner.
     */
    public synchronized CompletionStage<Void> activate(
        ConnectorProviderId providerId,
        ConnectorRuntimeContext context
    ) {
        Objects.requireNonNull(context, "runtime context must not be null");
        Objects.requireNonNull(providerId, "connector provider ID must not be null");
        ConnectorProvider<?> provider = providers.get(providerId);
        if (provider == null) {
            throw new IllegalStateException("no connector provider registered for ID: " + providerId.value());
        }
        if (state == LifecycleState.STOPPING || state == LifecycleState.STOPPED) {
            return failed("connector registry cannot activate a provider after stop has begun");
        }
        state = LifecycleState.RUNNING;
        return activations.computeIfAbsent(providerId, ignored -> startProvider(provider, context));
    }

    public synchronized CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        Objects.requireNonNull(context, "runtime context must not be null");
        if (state == LifecycleState.STOPPING || state == LifecycleState.STOPPED) {
            return lifecycle;
        }
        state = LifecycleState.STOPPING;
        CompletionStage<Void> settled = ConnectorCompletionStages.completed();
        for (CompletionStage<Void> activation : List.copyOf(activations.values())) {
            settled = settled.thenCompose(ignored -> activation.handle((activated, failure) -> null));
        }
        lifecycle = settled
            .thenCompose(ignored -> stopSequentially(context))
            .whenComplete((ignored, failure) -> markStopped());
        return lifecycle;
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

    private static List<ConnectorProvider<?>> reverse(List<ConnectorProvider<?>> providers) {
        List<ConnectorProvider<?>> reversed = new ArrayList<>(providers);
        java.util.Collections.reverse(reversed);
        return reversed;
    }

    private CompletionStage<Void> startProvider(
        ConnectorProvider<?> provider,
        ConnectorRuntimeContext context
    ) {
        final CompletionStage<Void> stage;
        try {
            stage = provider.start(context);
        } catch (RuntimeException failure) {
            return failed(failure);
        }
        if (stage == null) {
            return failed("connector provider " + provider.id().value() + " returned null from start");
        }
        return stage.thenRun(() -> recordStarted(provider));
    }

    private synchronized void recordStarted(ConnectorProvider<?> provider) {
        if (!startedProviders.contains(provider)) {
            startedProviders.add(provider);
        }
    }

    private synchronized void markStopped() {
        state = LifecycleState.STOPPED;
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
            stage = failed("connector provider " + provider.id().value() + " returned null from stop");
        }
        return stage.handle((ignored, failure) -> {
            synchronized (this) {
                if (failure == null) {
                    startedProviders.remove(provider);
                } else {
                    failures.add(new IllegalStateException(
                        "connector provider " + provider.id().value() + " failed to stop", failure));
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
        RUNNING,
        STOPPING,
        STOPPED
    }
}
