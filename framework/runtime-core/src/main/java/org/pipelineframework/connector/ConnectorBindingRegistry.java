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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Deterministic registry and lifecycle owner for named configured provider instances.
 */
public final class ConnectorBindingRegistry {
    private final Map<ConnectorBindingName, BindingSlot> bindings;
    private final Map<ConnectorBindingName, String> unavailableBindings;
    private final List<BindingSlot> bindingOrder;
    private final Map<BindingSlot, CompletionStage<Binding>> activations = new LinkedHashMap<>();
    private final Map<ConnectorBindingName, Binding> activeBindings = new LinkedHashMap<>();
    private CompletionStage<Void> lifecycle = ConnectorCompletionStages.completed();
    private LifecycleState state = LifecycleState.NEW;

    private ConnectorBindingRegistry(List<BindingSlot> bindings) {
        this(bindings, Map.of());
    }

    private ConnectorBindingRegistry(
        List<BindingSlot> bindings,
        Map<ConnectorBindingName, String> unavailableBindings
    ) {
        Map<ConnectorBindingName, BindingSlot> indexed = new LinkedHashMap<>();
        for (BindingSlot binding : bindings.stream().sorted(Comparator.comparing(BindingSlot::name)).toList()) {
            if (indexed.putIfAbsent(binding.name(), binding) != null) {
                throw new IllegalArgumentException("duplicate connector binding name: " + binding.name().value());
            }
        }
        this.bindings = Collections.unmodifiableMap(indexed);
        this.unavailableBindings = Map.copyOf(Objects.requireNonNull(
            unavailableBindings, "unavailable connector bindings must not be null"));
        this.bindingOrder = List.copyOf(indexed.values());
    }

    public static ConnectorBindingRegistry empty() {
        return new ConnectorBindingRegistry(List.of());
    }

    /**
     * Defines bindings from directly discovered provider prototypes. Binding-owned instances are
     * created lazily on first live activation.
     */
    public static ConnectorBindingRegistry fromProviders(
        Collection<ConnectorBindingDefinition> definitions,
        Collection<? extends ConnectorProvider<?>> providers
    ) {
        return fromProviders(definitions, providers, ConnectorProviderInstanceFactory.plainJava(), false);
    }

    /**
     * Retains definitions whose providers are absent so replay-only hosts can start without them.
     * Live activation or resolution of such a binding still fails deterministically.
     */
    public static ConnectorBindingRegistry fromProvidersAllowingUnavailable(
        Collection<ConnectorBindingDefinition> definitions,
        Collection<? extends ConnectorProvider<?>> providers
    ) {
        return fromProviders(definitions, providers, ConnectorProviderInstanceFactory.plainJava(), true);
    }

    static ConnectorBindingRegistry fromProviders(
        Collection<ConnectorBindingDefinition> definitions,
        Collection<? extends ConnectorProvider<?>> providers,
        ConnectorProviderInstanceFactory instanceFactory
    ) {
        return fromProviders(definitions, providers, instanceFactory, false);
    }

    static ConnectorBindingRegistry fromProvidersAllowingUnavailable(
        Collection<ConnectorBindingDefinition> definitions,
        Collection<? extends ConnectorProvider<?>> providers,
        ConnectorProviderInstanceFactory instanceFactory
    ) {
        return fromProviders(definitions, providers, instanceFactory, true);
    }

    private static ConnectorBindingRegistry fromProviders(
        Collection<ConnectorBindingDefinition> definitions,
        Collection<? extends ConnectorProvider<?>> providers,
        ConnectorProviderInstanceFactory instanceFactory,
        boolean allowUnavailable
    ) {
        Objects.requireNonNull(instanceFactory, "connector provider instance factory must not be null");
        List<ConnectorBindingDefinition> orderedDefinitions = orderedDefinitions(definitions);
        Map<ConnectorProviderId, ConnectorProvider<?>> available = indexedProviders(providers);
        List<BindingSlot> result = new ArrayList<>();
        Map<ConnectorBindingName, String> unavailable = new LinkedHashMap<>();
        for (ConnectorBindingDefinition definition : orderedDefinitions) {
            ConnectorProvider<?> prototype = available.get(definition.providerId());
            if (prototype == null) {
                String message = "connector binding '" + definition.name().value()
                    + "' has no runtime provider for ID: " + definition.providerId().value();
                if (allowUnavailable) {
                    unavailable.put(definition.name(), message);
                    continue;
                }
                throw new IllegalArgumentException(message);
            }
            result.add(new BindingSlot(definition, prototype, instanceFactory));
        }
        return new ConnectorBindingRegistry(result, unavailable);
    }

    /**
     * Activates every binding. Runtime hosts normally prefer {@link #activate} for lazy ownership.
     */
    public CompletionStage<Void> start(ConnectorRuntimeContext context) {
        Objects.requireNonNull(context, "runtime context must not be null");
        synchronized (this) {
            if (state == LifecycleState.STOPPING || state == LifecycleState.STOPPED) {
                return failed("connector binding registry cannot start after shutdown has begun");
            }
            state = LifecycleState.RUNNING;
        }
        CompletionStage<Void> sequence = ConnectorCompletionStages.completed();
        for (BindingSlot binding : bindingOrder) {
            sequence = sequence.thenCompose(ignored -> activate(binding.name(), context));
        }
        synchronized (this) {
            lifecycle = sequence;
        }
        return sequence;
    }

    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        Objects.requireNonNull(context, "runtime context must not be null");
        List<CompletionStage<Binding>> pending;
        CompletableFuture<Void> stopped = new CompletableFuture<>();
        synchronized (this) {
            if (state == LifecycleState.STOPPING || state == LifecycleState.STOPPED) {
                return lifecycle;
            }
            state = LifecycleState.STOPPING;
            pending = List.copyOf(activations.values());
            lifecycle = stopped;
        }
        CompletionStage<Void> settled = ConnectorCompletionStages.completed();
        for (CompletionStage<Binding> activation : pending) {
            settled = settled.thenCompose(ignored -> activation.handle(
                (activated, failure) -> ConnectorCompletionStages.completed()).thenCompose(stage -> stage));
        }
        settled
            .thenCompose(ignored -> stopStarted(context))
            .whenComplete((ignored, failure) -> {
                markStopped();
                if (failure == null) {
                    stopped.complete(null);
                } else {
                    stopped.completeExceptionally(failure);
                }
            });
        return stopped;
    }

    /**
     * Creates and starts one configured binding on first live use. Concurrent callers share the
     * same activation stage.
     */
    public CompletionStage<Void> activate(
        ConnectorBindingName name,
        ConnectorRuntimeContext context
    ) {
        Objects.requireNonNull(context, "runtime context must not be null");
        final BindingSlot slot;
        final CompletableFuture<Binding> activation;
        synchronized (this) {
            if (state == LifecycleState.STOPPING || state == LifecycleState.STOPPED) {
                return failed("connector binding registry cannot activate after shutdown has begun");
            }
            try {
                slot = requireSlot(name);
            } catch (RuntimeException failure) {
                return CompletableFuture.failedFuture(failure);
            }
            state = LifecycleState.RUNNING;
            CompletionStage<Binding> existing = activations.get(slot);
            if (existing != null) {
                return existing.thenApply(ignored -> null);
            }
            activation = new CompletableFuture<>();
            activations.put(slot, activation);
        }
        createAndStart(slot, context).whenComplete((binding, failure) ->
            completeActivation(slot, activation, binding, failure));
        return activation.thenApply(ignored -> null);
    }

    private void completeActivation(
        BindingSlot slot,
        CompletableFuture<Binding> activation,
        Binding binding,
        Throwable failure
    ) {
        if (failure == null) {
            activation.complete(binding);
            return;
        }
        synchronized (this) {
            if (state == LifecycleState.RUNNING) {
                activations.remove(slot, activation);
            }
        }
        activation.completeExceptionally(failure);
    }

    public Map<ConnectorBindingName, ConnectorProviderDescriptor> providers() {
        Map<ConnectorBindingName, ConnectorProviderDescriptor> result = new LinkedHashMap<>();
        bindings.forEach((name, binding) ->
            result.put(name, ConnectorDescriptors.provider(binding.prototype())));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Successfully activated provider instances, in deterministic binding order.
     */
    public synchronized List<ConnectorProvider<?>> providerInstances() {
        List<ConnectorProvider<?>> result = new ArrayList<>();
        for (BindingSlot slot : bindingOrder) {
            Binding binding = activeBindings.get(slot.name());
            if (binding != null) {
                result.add(binding.lease().provider());
            }
        }
        return List.copyOf(result);
    }

    public synchronized ConnectorProvider<?> requireProvider(ConnectorBindingName name) {
        return requireActiveBinding(name).lease().provider();
    }

    public synchronized ConnectorOperation requireOperation(
        ConnectorBindingName name,
        String operationId,
        ConnectorOperationKind kind,
        int operationMajorVersion
    ) {
        Binding binding = requireActiveBinding(name);
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            binding.lease().provider().id(), operationId, kind, operationMajorVersion);
        return binding.registry().requireOperation(identity);
    }

    public synchronized CommandOperation<?, ?, ?> requireCommandOperation(
        ConnectorBindingName name,
        String operationId,
        int operationMajorVersion,
        CommandPolicy policy
    ) {
        Binding binding = requireActiveBinding(name);
        ConnectorProvider<?> provider = binding.lease().provider();
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            provider.id(), operationId, ConnectorOperationKind.COMMAND, operationMajorVersion);
        return binding.registry().requireCommandOperation(
            identity, provider.version().major(), policy);
    }

    public synchronized QueryOperation<?, ?, ?> requireQueryOperation(
        ConnectorBindingName name,
        String operationId,
        int operationMajorVersion
    ) {
        Binding binding = requireActiveBinding(name);
        return binding.registry().requireExecutionOperation(
            new ConnectorOperationIdentity(
                binding.lease().provider().id(), operationId, ConnectorOperationKind.QUERY, operationMajorVersion),
            QueryOperation.class);
    }

    private BindingSlot requireSlot(ConnectorBindingName name) {
        Objects.requireNonNull(name, "connector binding name must not be null");
        BindingSlot binding = bindings.get(name);
        if (binding == null) {
            String unavailable = unavailableBindings.get(name);
            if (unavailable != null) {
                throw new IllegalStateException(unavailable + "; live connector execution is unavailable");
            }
            throw new IllegalStateException("no configured connector binding registered for name: " + name.value());
        }
        return binding;
    }

    private Binding requireActiveBinding(ConnectorBindingName name) {
        requireSlot(name);
        Binding binding = activeBindings.get(name);
        if (binding == null) {
            throw new IllegalStateException(
                "connector binding '" + name.value() + "' has not been activated for live execution");
        }
        return binding;
    }

    private CompletionStage<Binding> createAndStart(BindingSlot slot, ConnectorRuntimeContext context) {
        ConnectorProviderLease lease;
        try {
            lease = slot.instanceFactory().create(slot.prototype());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        final Binding binding;
        try {
            validateRuntimeIdentity(slot.prototype(), lease.provider());
            binding = bind(slot.definition(), lease);
        } catch (Throwable failure) {
            lease.release();
            return CompletableFuture.failedFuture(failure);
        }
        final CompletionStage<Void> activation;
        try {
            activation = binding.bound().start(context);
        } catch (Throwable failure) {
            lease.release();
            return CompletableFuture.failedFuture(failure);
        }
        if (activation == null) {
            lease.release();
            return CompletableFuture.failedFuture(new IllegalStateException(
                "connector binding '" + slot.name().value() + "' returned null from lifecycle start"));
        }
        return activation.thenApply(ignored -> {
            markStarted(binding);
            return binding;
        }).whenComplete((ignored, failure) -> {
            if (failure != null) {
                lease.release();
            }
        });
    }

    private synchronized void markStarted(Binding binding) {
        activeBindings.putIfAbsent(binding.name(), binding);
    }

    private synchronized void markStopped() {
        state = LifecycleState.STOPPED;
    }

    private CompletionStage<Void> stopStarted(ConnectorRuntimeContext context) {
        List<Binding> reverse;
        synchronized (this) {
            reverse = bindingOrder.stream()
                .map(BindingSlot::name)
                .map(activeBindings::get)
                .filter(Objects::nonNull)
                .toList();
        }
        reverse = new ArrayList<>(reverse);
        Collections.reverse(reverse);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        CompletionStage<Void> sequence = ConnectorCompletionStages.completed();
        for (Binding binding : reverse) {
            sequence = sequence.thenCompose(ignored -> stop(binding, context)
                .handle((stopped, failure) -> {
                    if (failure != null) {
                        firstFailure.compareAndSet(null, failure);
                    }
                    return null;
                }));
        }
        return sequence.thenCompose(ignored -> firstFailure.get() == null
            ? ConnectorCompletionStages.completed()
            : CompletableFuture.failedFuture(firstFailure.get()));
    }

    private CompletionStage<Void> stop(Binding binding, ConnectorRuntimeContext context) {
        final CompletionStage<Void> stopped;
        try {
            stopped = binding.bound().stop(context);
        } catch (Throwable failure) {
            binding.lease().release();
            return CompletableFuture.failedFuture(failure);
        }
        if (stopped == null) {
            binding.lease().release();
            return CompletableFuture.failedFuture(new IllegalStateException(
                "connector binding '" + binding.name().value() + "' returned null from lifecycle stop"));
        }
        return stopped.whenComplete((ignored, failure) -> binding.lease().release());
    }

    private static Binding bind(ConnectorBindingDefinition definition, ConnectorProviderLease lease) {
        ConnectorRegistry registry = new ConnectorRegistry(List.of(lease.provider()));
        registry.requireProvider(definition.providerId(), definition.providerMajorVersion());
        BoundConnectorRegistry bound = registry.bind(new ConnectorProviderConfigurations(Map.of(
            definition.providerId(), definition.configuration())));
        return new Binding(definition.name(), lease, registry, bound);
    }

    private static void validateRuntimeIdentity(
        ConnectorProvider<?> prototype,
        ConnectorProvider<?> runtimeProvider
    ) {
        ConnectorProviderDescriptor expectedProvider = ConnectorDescriptors.provider(prototype);
        ConnectorProviderDescriptor actualProvider = ConnectorDescriptors.provider(runtimeProvider);
        if (!expectedProvider.equals(actualProvider)) {
            throw new IllegalStateException(
                "binding-owned connector provider identity differs from discovery metadata: expected "
                    + expectedProvider + ", got " + actualProvider);
        }
        List<ConnectorOperationDescriptor> expectedOperations = operationDescriptors(prototype);
        List<ConnectorOperationDescriptor> actualOperations = operationDescriptors(runtimeProvider);
        if (!expectedOperations.equals(actualOperations)) {
            throw new IllegalStateException(
                "binding-owned connector operation metadata differs from discovery metadata for "
                    + expectedProvider.id().value());
        }
    }

    private static List<ConnectorOperationDescriptor> operationDescriptors(ConnectorProvider<?> provider) {
        return provider.operations().stream()
            .map(ConnectorDescriptors::operation)
            .sorted(Comparator
                .comparing(ConnectorOperationDescriptor::kind)
                .thenComparing(ConnectorOperationDescriptor::id)
                .thenComparingInt(ConnectorOperationDescriptor::majorVersion))
            .toList();
    }

    private static List<ConnectorBindingDefinition> orderedDefinitions(
        Collection<ConnectorBindingDefinition> definitions
    ) {
        Objects.requireNonNull(definitions, "connector binding definitions must not be null");
        List<ConnectorBindingDefinition> ordered = definitions.stream()
            .map(definition -> Objects.requireNonNull(definition, "connector binding definition must not be null"))
            .sorted(Comparator.comparing(ConnectorBindingDefinition::name))
            .toList();
        for (int index = 1; index < ordered.size(); index++) {
            if (ordered.get(index - 1).name().equals(ordered.get(index).name())) {
                throw new IllegalArgumentException("duplicate connector binding name: " + ordered.get(index).name().value());
            }
        }
        return ordered;
    }

    private static Map<ConnectorProviderId, ConnectorProvider<?>> indexedProviders(
        Collection<? extends ConnectorProvider<?>> providers
    ) {
        Objects.requireNonNull(providers, "connector providers must not be null");
        List<ConnectorProvider<?>> ordered = new ArrayList<>();
        for (ConnectorProvider<?> provider : providers) {
            ordered.add(Objects.requireNonNull(provider, "connector provider must not be null"));
        }
        ordered.sort(Comparator.comparing(ConnectorProvider::id));
        Map<ConnectorProviderId, ConnectorProvider<?>> result = new LinkedHashMap<>();
        for (ConnectorProvider<?> provider : ordered) {
            ConnectorProviderId id = provider.id();
            if (result.putIfAbsent(id, provider) != null) {
                throw new IllegalArgumentException("duplicate connector provider ID: " + id.value());
            }
        }
        return result;
    }

    private static <T> CompletionStage<T> failed(String message) {
        return CompletableFuture.failedFuture(new IllegalStateException(message));
    }

    private record BindingSlot(
        ConnectorBindingDefinition definition,
        ConnectorProvider<?> prototype,
        ConnectorProviderInstanceFactory instanceFactory
    ) {
        private ConnectorBindingName name() {
            return definition.name();
        }
    }

    private record Binding(
        ConnectorBindingName name,
        ConnectorProviderLease lease,
        ConnectorRegistry registry,
        BoundConnectorRegistry bound
    ) {
    }

    private enum LifecycleState {
        NEW,
        RUNNING,
        STOPPING,
        STOPPED
    }
}
