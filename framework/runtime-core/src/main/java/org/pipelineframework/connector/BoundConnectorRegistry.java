package org.pipelineframework.connector;

import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Explicitly bound provider configurations ready for lifecycle start.
 */
public final class BoundConnectorRegistry {
    private final ConnectorRegistry registry;
    private final Map<ConnectorProviderId, Object> providerConfigurations;
    private final List<ConnectorProvider<?>> startedProviders = new ArrayList<>();
    private CompletionStage<Void> lifecycle = ConnectorCompletionStages.completed();
    private boolean started;

    BoundConnectorRegistry(ConnectorRegistry registry, Map<ConnectorProviderId, Object> providerConfigurations) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.providerConfigurations = Map.copyOf(new LinkedHashMap<>(providerConfigurations));
    }

    public synchronized CompletionStage<Void> start(ConnectorRuntimeContext context) {
        Objects.requireNonNull(context, "runtime context must not be null");
        if (!started) {
            started = true;
            CompletionStage<Void> sequence = ConnectorCompletionStages.completed();
            for (ConnectorProvider<?> provider : registry.providerOrder()) {
                sequence = sequence.thenCompose(ignored -> start(provider, providerConfigurations, context)
                    .thenRun(() -> startedProviders.add(provider)));
            }
            lifecycle = sequence;
        }
        return lifecycle;
    }

    public synchronized CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        Objects.requireNonNull(context, "runtime context must not be null");
        List<ConnectorProvider<?>> reverse = new ArrayList<>(startedProviders);
        Collections.reverse(reverse);
        CompletionStage<Void> sequence = lifecycle.handle((ignored, failure) -> ConnectorCompletionStages.completed())
            .thenCompose(ignored -> ignored);
        for (ConnectorProvider<?> provider : reverse) {
            sequence = sequence.thenCompose(ignored -> provider.stop(context));
        }
        lifecycle = sequence;
        return lifecycle;
    }

    public ConnectorRegistry registry() {
        return registry;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static CompletionStage<Void> start(
        ConnectorProvider provider,
        Map<ConnectorProviderId, Object> configurations,
        ConnectorRuntimeContext context
    ) {
        CompletionStage<Void> stage = configurations.containsKey(provider.id())
            ? provider.start(context, configurations.get(provider.id()))
            : provider.start(context);
        if (stage == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "connector provider " + provider.id().value() + " returned null from start"));
        }
        return stage;
    }
}
