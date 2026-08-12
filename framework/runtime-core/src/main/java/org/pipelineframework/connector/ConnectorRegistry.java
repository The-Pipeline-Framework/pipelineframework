package org.pipelineframework.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        Objects.requireNonNull(discoveredProviders, "providers must not be null");
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
                .whenComplete((ignored, failure) -> state = failure == null ? LifecycleState.STARTED : LifecycleState.FAILED);
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
                .thenCompose(ignored -> sequentially(reverse(startedProviders), provider -> provider.stop(context), "stop"))
                .whenComplete((ignored, failure) -> state = failure == null ? LifecycleState.STOPPED : LifecycleState.FAILED);
        }
        return lifecycle;
    }

    private static boolean isExecutionKindSupported(ConnectorOperationKind kind) {
        return ConnectorOperationKind.COMMAND.equals(kind) || ConnectorOperationKind.QUERY.equals(kind);
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
                return stage.thenRun(() -> startedProviders.add(provider));
            });
        }
        return sequence;
    }

    private static CompletionStage<Void> sequentially(
        Collection<ConnectorProvider<?>> providers,
        ProviderLifecycleAction action,
        String actionName
    ) {
        CompletionStage<Void> sequence = ConnectorCompletionStages.completed();
        for (ConnectorProvider<?> provider : providers) {
            sequence = sequence.thenCompose(ignored -> {
                CompletionStage<Void> stage = action.apply(provider);
                if (stage == null) {
                    return failed("connector provider " + provider.descriptor().id().value() + " returned null from " + actionName);
                }
                return stage;
            });
        }
        return sequence;
    }

    private static CompletionStage<Void> failed(String message) {
        return CompletableFuture.failedFuture(new IllegalStateException(message));
    }

    @FunctionalInterface
    private interface ProviderLifecycleAction {
        CompletionStage<Void> apply(ConnectorProvider<?> provider);
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
