package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBindingRegistryTest {
    private static final ConnectorConfigSchema<ProviderConfig> SCHEMA =
        ConnectorConfigSchema.record(ProviderConfig.class, "acme.binding", 1);
    private static final List<String> EVENTS = new ArrayList<>();

    @BeforeEach
    void resetEvents() {
        EVENTS.clear();
    }

    @Test
    void createsBindingOwnedProvidersAndStartsThemInBindingOrder() {
        AlphaProvider alphaPrototype = new AlphaProvider();
        BetaProvider betaPrototype = new BetaProvider();
        ConnectorBindingRegistry registry = ConnectorBindingRegistry.fromProviders(
            List.of(definition("zeta", "acme.beta", "beta-secret"), definition("alpha", "acme.alpha", "alpha-secret")),
            List.of(betaPrototype, alphaPrototype));

        assertTrue(registry.providerInstances().isEmpty());
        assertTrue(EVENTS.isEmpty());

        registry.start(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        assertNotSame(alphaPrototype, registry.providerInstances().get(0));
        assertNotSame(betaPrototype, registry.providerInstances().get(1));
        registry.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();

        assertEquals(List.of(
            "start:acme.alpha:alpha-secret",
            "start:acme.beta:beta-secret",
            "stop:acme.beta",
            "stop:acme.alpha"), EVENTS);
    }

    @Test
    void repeatedBindingsCreateDistinctInstancesWithoutAnAuthorFactory() {
        SharedProvider prototype = new SharedProvider();
        ConnectorBindingRegistry registry = ConnectorBindingRegistry.fromProviders(
            List.of(definition("first", "acme.shared", "one"), definition("second", "acme.shared", "two")),
            List.of(prototype));

        assertTrue(registry.providerInstances().isEmpty());

        registry.activate(ConnectorBindingName.of("second"), ConnectorRuntimeContext.empty())
            .toCompletableFuture().join();
        ConnectorProvider<?> second = registry.providerInstances().get(0);
        registry.activate(ConnectorBindingName.of("first"), ConnectorRuntimeContext.empty())
            .toCompletableFuture().join();
        List<ConnectorProvider<?>> instances = registry.providerInstances();

        assertEquals(2, instances.size());
        assertNotSame(prototype, instances.get(0));
        assertNotSame(instances.get(0), instances.get(1));
        assertEquals(second, instances.get(1));
        assertEquals(List.of("start:acme.shared:two", "start:acme.shared:one"), EVENTS);
        assertEquals("one", ((TestProvider) instances.get(0)).config.secret().value());
        assertEquals("two", ((TestProvider) instances.get(1)).config.secret().value());
    }

    @Test
    void liveBindingCreationRequiresThePackagingConstructorContract() {
        ConnectorBindingRegistry registry = ConnectorBindingRegistry.fromProviders(
            List.of(definition("private", "acme.private", "one")),
            List.of(new PrivateProvider("ignored")));
        RuntimeException activation = assertThrows(RuntimeException.class, () -> registry.activate(
            ConnectorBindingName.of("private"), ConnectorRuntimeContext.empty()).toCompletableFuture().join());
        Throwable failure = activation.getCause();

        assertTrue(failure.getMessage().contains("public no-argument constructor"), failure.getMessage());
    }

    @Test
    void releasesBindingOwnedProviderExactlyOnceWhenActivationFails() {
        AtomicInteger releases = new AtomicInteger();
        ConnectorProviderInstanceFactory factory = ignored -> ConnectorProviderLease.of(
            new FailingSharedProvider(), releases::incrementAndGet);
        ConnectorBindingRegistry registry = ConnectorBindingRegistry.fromProviders(
            List.of(definition("failing", "acme.shared", "one")),
            List.of(new SharedProvider()),
            factory);

        assertThrows(RuntimeException.class, () -> registry.activate(
            ConnectorBindingName.of("failing"), ConnectorRuntimeContext.empty()).toCompletableFuture().join());

        assertEquals(1, releases.get());
    }

    @Test
    void providerCreationDoesNotHoldTheRegistryMonitorDuringShutdown() throws Exception {
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch releaseCreation = new CountDownLatch(1);
        ConnectorProviderInstanceFactory factory = ignored -> {
            creating.countDown();
            try {
                if (!releaseCreation.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("provider creation was not released");
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("provider creation interrupted", failure);
            }
            return ConnectorProviderLease.of(new SharedProvider());
        };
        ConnectorBindingRegistry registry = ConnectorBindingRegistry.fromProviders(
            List.of(definition("blocking", "acme.shared", "one")), List.of(new SharedProvider()), factory);

        CompletableFuture<CompletionStage<Void>> activation = CompletableFuture.supplyAsync(() ->
            registry.activate(ConnectorBindingName.of("blocking"), ConnectorRuntimeContext.empty()));
        assertTrue(creating.await(2, TimeUnit.SECONDS));
        CompletionStage<Void> stopped = CompletableFuture.supplyAsync(() ->
            registry.stop(ConnectorRuntimeContext.empty())).get(2, TimeUnit.SECONDS);
        releaseCreation.countDown();

        activation.join().toCompletableFuture().join();
        stopped.toCompletableFuture().join();
    }

    @Test
    void replayOnlyHostsCanRetainUnavailableBindingsWithoutStartingThem() {
        ConnectorBindingRegistry registry = ConnectorBindingRegistry.fromProvidersAllowingUnavailable(
            List.of(definition("offline", "acme.missing", "one")),
            List.of());

        registry.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
            registry.requireProvider(ConnectorBindingName.of("offline")));
        assertTrue(failure.getMessage().contains("live connector execution is unavailable"), failure.getMessage());
    }

    @Test
    void staticSchemaValidationAcceptsDurationAndReferencesWithoutResolution() {
        ConnectorConfigurationDocument document = configuration("secret-name");

        ConnectorConfigurationValidator.validate(SCHEMA.descriptor(), document, "binding test");

        ConnectorConfigurationException failure = assertThrows(ConnectorConfigurationException.class, () ->
            ConnectorConfigurationValidator.validate(
                SCHEMA.descriptor(),
                new ConnectorConfigurationDocument(Map.of(
                    "timeout", "five seconds",
                    "connection", "connection-name",
                    "secret", "secret-name")),
                "binding test"));
        assertTrue(failure.getMessage().contains("field 'timeout'"), failure.getMessage());
    }

    private static ConnectorBindingDefinition definition(String binding, String provider, String secret) {
        return new ConnectorBindingDefinition(
            ConnectorBindingName.of(binding),
            ConnectorProviderId.of(provider),
            1,
            configuration(secret));
    }

    private static ConnectorConfigurationDocument configuration(String secret) {
        return new ConnectorConfigurationDocument(Map.of(
            "timeout", "PT5S",
            "connection", "alpha-connection",
            "secret", secret));
    }

    public record ProviderConfig(Duration timeout, ConnectionRef connection, SecretRef secret) {
    }

    private abstract static class TestProvider implements ConnectorProvider<ProviderConfig> {
        private ProviderConfig config;

        @Override
        public ConnectorProviderVersion version() {
            return new ConnectorProviderVersion(1, 0);
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of();
        }

        @Override
        public Optional<ConnectorConfigSchema<ProviderConfig>> configurationSchema() {
            return Optional.of(SCHEMA);
        }

        @Override
        public CompletionStage<Void> start(ConnectorRuntimeContext context) {
            return CompletableFuture.failedFuture(new AssertionError("bound provider must receive typed config"));
        }

        @Override
        public CompletionStage<Void> start(ConnectorRuntimeContext context, ProviderConfig configuration) {
            config = configuration;
            EVENTS.add("start:" + id().value() + ":" + configuration.secret().value());
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
            EVENTS.add("stop:" + id().value());
            return CompletableFuture.completedFuture(null);
        }
    }

    public static final class AlphaProvider extends TestProvider {
        public AlphaProvider() {
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.alpha");
        }
    }

    public static final class BetaProvider extends TestProvider {
        public BetaProvider() {
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.beta");
        }
    }

    public static final class SharedProvider extends TestProvider {
        public SharedProvider() {
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.shared");
        }
    }

    private static final class FailingSharedProvider extends TestProvider {
        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.shared");
        }

        @Override
        public CompletionStage<Void> start(ConnectorRuntimeContext context, ProviderConfig configuration) {
            return CompletableFuture.failedFuture(new IllegalStateException("start failed"));
        }
    }

    private static final class PrivateProvider extends TestProvider {
        private PrivateProvider(String ignored) {
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.private");
        }
    }
}
