package org.pipelineframework.connector;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
@IfBuildProperty(name = "connector.cdi.lifecycle.test", stringValue = "true")
final class InjectedConnectorProvider implements ConnectorProvider<InjectedConnectorProvider.BindingConfig> {
    private static final ConnectorProviderId ID = ConnectorProviderId.of("test.cdi");
    private static final ConnectorConfigSchema<BindingConfig> PROVIDER_SCHEMA =
        ConnectorConfigSchema.record(BindingConfig.class, "test.cdi.provider", 1);
    private static final ConnectorConfigSchema<OperationConfig> OPERATION_SCHEMA =
        ConnectorConfigSchema.record(OperationConfig.class, "test.cdi.operation", 1);
    private static final AtomicInteger INSTANCE_SEQUENCE = new AtomicInteger();
    private static final AtomicInteger UNCONFIGURED_STARTS = new AtomicInteger();
    private static final Map<String, AtomicInteger> CONFIGURATION_BINDINGS = new ConcurrentHashMap<>();
    private static final Map<String, Integer> BINDING_INSTANCES = new ConcurrentHashMap<>();
    private static final Map<Integer, AtomicInteger> STARTS = new ConcurrentHashMap<>();
    private static final Map<Integer, AtomicInteger> STOPS = new ConcurrentHashMap<>();
    private static final AtomicReference<CompletableFuture<Void>> RACING_START =
        new AtomicReference<>(new CompletableFuture<>());

    private final int instanceId = INSTANCE_SEQUENCE.incrementAndGet();

    @Inject
    InjectedConnectorDependency dependency;

    private volatile BindingConfig bindingConfig;

    @Override
    public ConnectorProviderId id() {
        return ID;
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return List.of(new InspectionOperation("inspect.first"), new InspectionOperation("inspect.second"));
    }

    @Override
    public Optional<ConnectorConfigSchema<BindingConfig>> configurationSchema() {
        return Optional.of(PROVIDER_SCHEMA);
    }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context) {
        UNCONFIGURED_STARTS.incrementAndGet();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, BindingConfig configuration) {
        if (dependency == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("CDI dependency was not injected"));
        }
        bindingConfig = configuration;
        CONFIGURATION_BINDINGS.computeIfAbsent(configuration.name(), ignored -> new AtomicInteger()).incrementAndGet();
        BINDING_INSTANCES.put(configuration.name(), instanceId);
        STARTS.computeIfAbsent(instanceId, ignored -> new AtomicInteger()).incrementAndGet();
        return "racing".equals(configuration.name())
            ? RACING_START.get()
            : CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        STOPS.computeIfAbsent(instanceId, ignored -> new AtomicInteger()).incrementAndGet();
        return CompletableFuture.completedFuture(null);
    }

    static int unconfiguredStarts() {
        return UNCONFIGURED_STARTS.get();
    }

    static int configurationBindings(String name) {
        AtomicInteger count = CONFIGURATION_BINDINGS.get(name);
        return count == null ? 0 : count.get();
    }

    static int instanceFor(String binding) {
        Integer instance = BINDING_INSTANCES.get(binding);
        if (instance == null) {
            throw new IllegalStateException("binding has not started: " + binding);
        }
        return instance;
    }

    static int starts(int instance) {
        AtomicInteger count = STARTS.get(instance);
        return count == null ? 0 : count.get();
    }

    static int stops(int instance) {
        AtomicInteger count = STOPS.get(instance);
        return count == null ? 0 : count.get();
    }

    static void releaseRacingStart() {
        RACING_START.get().complete(null);
    }

    static void resetObservations() {
        UNCONFIGURED_STARTS.set(0);
        CONFIGURATION_BINDINGS.clear();
        BINDING_INSTANCES.clear();
        STARTS.clear();
        STOPS.clear();
        RACING_START.set(new CompletableFuture<>());
    }

    record BindingConfig(String name) {
    }

    record OperationConfig(String suffix) {
    }

    record InvocationResult(int providerInstance, String binding, String operation, String injection, String suffix) {
    }

    private final class InspectionOperation implements CommandOperation<String, OperationConfig, InvocationResult> {
        private final String operationId;

        private InspectionOperation(String operationId) {
            this.operationId = operationId;
        }

        @Override
        public String id() {
            return operationId;
        }

        @Override
        public Optional<ConnectorConfigSchema<OperationConfig>> configurationSchema() {
            return Optional.of(OPERATION_SCHEMA);
        }

        @Override
        public CompletionStage<CommandOutcome<InvocationResult>> dispatch(
            CommandInvocation<String, OperationConfig> invocation
        ) {
            BindingConfig activeConfiguration = bindingConfig;
            if (activeConfiguration == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("provider binding is not active"));
            }
            InvocationResult result = new InvocationResult(
                instanceId,
                activeConfiguration.name(),
                operationId,
                dependency.marker(),
                invocation.configuration().suffix());
            return CompletableFuture.completedFuture(
                new CommandOutcome.Succeeded<>(result, CommandConfirmation.none(), List.of()));
        }
    }
}

@ApplicationScoped
@IfBuildProperty(name = "connector.cdi.lifecycle.test", stringValue = "true")
final class InjectedConnectorDependency {
    String marker() {
        return "injected";
    }
}

@ApplicationScoped
@IfBuildProperty(name = "connector.cdi.lifecycle.test", stringValue = "true")
final class InjectedConnectorPipelineConfig {
    private String previous;

    void configure(@Observes @Priority(1) StartupEvent event) {
        previous = System.getProperty("pipeline.config");
        URL resource = InjectedConnectorPipelineConfig.class.getResource("/connector-binding-cdi-pipeline.yaml");
        if (resource == null) {
            throw new IllegalStateException("connector CDI lifecycle pipeline fixture is unavailable");
        }
        try {
            System.setProperty("pipeline.config", Path.of(resource.toURI()).toString());
        } catch (URISyntaxException failure) {
            throw new IllegalStateException("connector CDI lifecycle pipeline fixture path is invalid", failure);
        }
    }

    void clear(@Observes ShutdownEvent event) {
        if (previous == null) {
            System.clearProperty("pipeline.config");
        } else {
            System.setProperty("pipeline.config", previous);
        }
    }
}
