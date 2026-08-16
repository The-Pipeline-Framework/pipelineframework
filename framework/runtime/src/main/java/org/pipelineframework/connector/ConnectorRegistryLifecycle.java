package org.pipelineframework.connector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.pipelineframework.command.CommandConnector;
import org.pipelineframework.command.LegacyCommandConnectorProvider;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;

/**
 * Quarkus/CDI lifecycle adapter. Provider implementations still use only the host-neutral core SPI.
 */
@ApplicationScoped
public class ConnectorRegistryLifecycle {
    @Inject
    Instance<ConnectorProvider<?>> providerInstances;

    @Inject
    Instance<CommandConnector<?, ?>> legacyCommandConnectors;

    @Inject
    ConnectorRuntimeContext runtimeContext;

    @Inject
    QuarkusConnectorProviderInstanceFactory providerInstanceFactory;

    private ConnectorRegistry registry;
    private Optional<ConnectorBindingRegistry> bindingRegistry = Optional.empty();

    void onStart(@Observes StartupEvent event) {
        List<ConnectorProvider<?>> providers = providerInstances.stream().toList();
        List<CommandConnector<?, ?>> legacy = legacyCommandConnectors.stream().toList();
        registry = createRegistry(providers, legacy);
        List<ConnectorBindingDefinition> definitions = loadBindingDefinitions();
        ConnectorBindingRegistry configuredBindings = createBindingRegistry(
            definitions, providers, providerInstanceFactory);
        bindingRegistry = Optional.of(configuredBindings);
    }

    void onStop(@Observes ShutdownEvent event) {
        if (registry != null) {
            stopAll(
                bindingRegistry.orElse(ConnectorBindingRegistry.empty()).stop(runtimeContext),
                registry.stop(runtimeContext))
                .toCompletableFuture()
                .join();
        }
    }

    @Produces
    @ApplicationScoped
    ConnectorRegistry registry() {
        if (registry == null) {
            throw new IllegalStateException("connector registry is not available before Quarkus startup");
        }
        return registry;
    }

    @Produces
    @ApplicationScoped
    ConnectorBindingRegistry bindingRegistry() {
        return bindingRegistry.orElseThrow(() ->
            new IllegalStateException("connector binding registry is not available before Quarkus startup"));
    }

    public static ConnectorRegistry createRegistry(Collection<? extends ConnectorProvider<?>> providers) {
        return new ConnectorRegistry(providers);
    }

    public static ConnectorRegistry createRegistry(
        Collection<? extends ConnectorProvider<?>> providers,
        Collection<? extends CommandConnector<?, ?>> legacyConnectors
    ) {
        return LegacyCommandConnectorProvider.createRegistry(providers, legacyConnectors);
    }

    public static ConnectorBindingRegistry createBindingRegistry(
        Collection<ConnectorBindingDefinition> definitions,
        Collection<? extends ConnectorProvider<?>> providers
    ) {
        if (definitions == null || definitions.isEmpty()) {
            return ConnectorBindingRegistry.empty();
        }
        return ConnectorBindingRegistry.fromProviders(
            definitions,
            providers == null ? List.of() : providers);
    }

    static ConnectorBindingRegistry createBindingRegistry(
        Collection<ConnectorBindingDefinition> definitions,
        Collection<? extends ConnectorProvider<?>> providers,
        ConnectorProviderInstanceFactory instanceFactory
    ) {
        if (definitions == null || definitions.isEmpty()) {
            return ConnectorBindingRegistry.empty();
        }
        return ConnectorBindingRegistry.fromProviders(
            definitions,
            providers == null ? List.of() : providers,
            instanceFactory);
    }

    private static CompletionStage<Void> stopAll(CompletionStage<Void>... stages) {
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        CompletionStage<Void> sequence = ConnectorCompletionStages.completed();
        for (CompletionStage<Void> stage : stages) {
            sequence = sequence.thenCompose(ignored -> stage.handle((stopped, failure) -> {
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

    private static List<ConnectorBindingDefinition> loadBindingDefinitions() {
        Optional<Path> configPath = configuredPipelinePath();
        if (configPath.isEmpty()) {
            return List.of();
        }
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath.orElseThrow());
        List<ConnectorBindingDefinition> definitions = new ArrayList<>();
        config.connectors().values().stream()
            .sorted(java.util.Comparator.comparing(binding -> binding.name()))
            .map(binding -> binding.toDefinition())
            .forEach(definitions::add);
        return List.copyOf(definitions);
    }

    private static Optional<Path> configuredPipelinePath() {
        Optional<String> explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit.isPresent()) {
            Path path = Path.of(explicit.orElseThrow()).toAbsolutePath().normalize();
            if (Files.isDirectory(path)) {
                return new PipelineYamlConfigLocator().locate(path);
            }
            return Optional.of(path);
        }
        return new PipelineYamlConfigLocator().locate(Path.of("").toAbsolutePath());
    }

    private static Optional<String> firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return Optional.of(value.trim());
            }
        }
        return Optional.empty();
    }
}
