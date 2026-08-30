package org.pipelineframework.connector;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorConfigurationBindingTest {

    @Test
    void bindsTypedProviderAndOperationRecordsBeforeTheirBoundaries() {
        AtomicInteger connectionResolutions = new AtomicInteger();
        AtomicInteger secretResolutions = new AtomicInteger();
        FakeProvider provider = new FakeProvider();
        ConnectorRegistry registry = new ConnectorRegistry(List.of(provider));
        ConnectorRuntimeContext context = context(connectionResolutions, secretResolutions);

        BoundConnectorRegistry bound = registry.bind(new ConnectorProviderConfigurations(Map.of(
            ConnectorProviderId.of("fake.config"), new ConnectorConfigurationDocument(Map.of(
                "connection", "search-primary",
                "secret", "search-token",
                "timeout", "PT5S",
                "label", "primary")))));

        assertEquals(0, connectionResolutions.get());
        assertEquals(0, secretResolutions.get());
        bound.start(context).toCompletableFuture().join();
        assertEquals(new ProviderConfig(new ConnectionRef("search-primary"), new SecretRef("search-token"), Duration.ofSeconds(5), Optional.of("primary")), provider.startedWith);
        assertEquals(1, connectionResolutions.get());
        assertEquals(0, secretResolutions.get());

        provider.operation.query(
            "input",
            new ConnectorConfigurationDocument(Map.of("index", "orders", "limit", 20, "mode", "FAST")),
            String.class,
            ConnectorExecutionContext.empty()).toCompletableFuture().join();

        assertEquals(new OperationConfig("orders", 20, Mode.FAST), provider.operation.invokedWith);
        assertEquals(1, secretResolutions.get());
    }

    @Test
    void rejectsInvalidOperationConfigBeforeInvocationWithActionableDiagnostics() {
        FakeProvider provider = new FakeProvider();
        new ConnectorRegistry(List.of(provider));

        ConnectorConfigurationException failure = assertThrows(ConnectorConfigurationException.class, () -> provider.operation.query(
            "input",
            new ConnectorConfigurationDocument(Map.of("index", "orders", "limit", "not-an-integer", "mode", "FAST")),
            String.class,
            ConnectorExecutionContext.empty()));

        assertTrue(failure.getMessage().contains("fake.config.query"));
        assertTrue(failure.getMessage().contains("field 'limit'"));
        assertEquals(0, provider.operation.invocations.get());
    }

    @Test
    void publishesInlineSchemaAndRedactsSecretReferencesFromSnapshots() {
        ConnectorConfigSchema<ProviderConfig> schema = providerSchema();
        ConnectorConfigurationDocument document = new ConnectorConfigurationDocument(Map.of(
            "connection", "search-primary",
            "secret", "literal-secret-reference",
            "timeout", "PT5S",
            "label", "primary"));
        ConnectorConfigurationSnapshot snapshot = ConnectorConfigurationSnapshot.from(schema, document, true);
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(new java.io.ByteArrayInputStream("""
            {"schemaVersion":1,"providers":[{"id":"fake.config","version":{"major":1,"minor":0},
            "configurationSchema":{"id":"fake.config.provider","version":1,"fields":[
            {"name":"connection","type":"CONNECTION_REF","required":true},
            {"name":"secret","type":"SECRET_REF","required":true},
            {"name":"timeout","type":"DURATION","required":true},
            {"name":"label","type":"STRING","required":false}]},"operations":[]}]}
            """.getBytes(java.nio.charset.StandardCharsets.UTF_8)));

        assertEquals(4, manifest.providers().getFirst().provider().configurationSchema().orElseThrow().fields().size());
        assertEquals(List.of(new ConnectionRef("search-primary")), snapshot.connectionReferences());
        assertFalse(snapshot.toString().contains("literal-secret-reference"));
        assertFalse(manifest.toString().contains("literal-secret-reference"));
    }

    @Test
    void bindsMapConfigurationIncludingNestedImmutableRecords() {
        ConnectorConfigSchema<NestedConfig> schema =
            ConnectorConfigSchema.record(NestedConfig.class, "fake.config.nested", 1);
        ConnectorConfigurationDocument document = new ConnectorConfigurationDocument(Map.of(
            "predicates", Map.of("customerId", Map.of(
                "operator", "eq",
                "values", List.of("input.customerId"))),
            "projection", Map.of("customerId", "customerId")));

        ConnectorConfigurationValidator.validate(schema.descriptor(), document, "nested operation");
        NestedConfig configuration = ConnectorConfigurationBinder.bind(schema, document, "nested operation");

        assertEquals(
            new NestedPredicate("eq", List.of("input.customerId")),
            configuration.predicates().get("customerId"));
        assertEquals(Map.of("customerId", "customerId"), configuration.projection().orElseThrow());
        assertEquals(ConnectorConfigValueType.MAP, schema.descriptor().fields().getFirst().type());

        ConnectorConfigurationException failure = assertThrows(ConnectorConfigurationException.class, () ->
            ConnectorConfigurationValidator.validate(
                schema.descriptor(),
                new ConnectorConfigurationDocument(Map.of("predicates", "not-a-map")),
                "nested operation"));
        assertTrue(failure.getMessage().contains("expected MAP value"));
    }

    private static ConnectorRuntimeContext context(AtomicInteger connections, AtomicInteger secrets) {
        ConnectionResolver connectionResolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                connections.incrementAndGet();
                return CompletableFuture.supplyAsync(
                    () -> request.connectionType().cast(new FakeConnection()), Runnable::run);
            }
        };
        SecretResolver secretResolver = reference -> {
            secrets.incrementAndGet();
            return CompletableFuture.supplyAsync(FakeSecret::new, Runnable::run);
        };
        Executor executor = Runnable::run;
        return ConnectorRuntimeContext.of(
            "test", executor, Clock.systemUTC(), Optional.of(connectionResolver), Optional.of(secretResolver));
    }

    private static ConnectorConfigSchema<ProviderConfig> providerSchema() {
        return ConnectorConfigSchema.record(ProviderConfig.class, "fake.config.provider", 1);
    }

    private static ConnectorConfigSchema<OperationConfig> operationSchema() {
        return ConnectorConfigSchema.record(OperationConfig.class, "fake.config.query", 1);
    }

    record ProviderConfig(ConnectionRef connection, SecretRef secret, Duration timeout, Optional<String> label) {
    }

    record OperationConfig(String index, int limit, Mode mode) {
    }

    record NestedConfig(
        Map<String, NestedPredicate> predicates,
        Optional<Map<String, String>> projection
    ) {
    }

    record NestedPredicate(String operator, List<Object> values) {
    }

    private enum Mode {
        FAST,
        SAFE
    }

    private static final class FakeConnection implements ResolvedConnection {
    }

    private static final class FakeSecret implements ResolvedSecret {
    }

    private static final class FakeProvider implements ConnectorProvider<ProviderConfig> {
        private final FakeQuery operation = new FakeQuery();
        private ProviderConfig startedWith;

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("fake.config");
        }

        @Override
        public ConnectorProviderVersion version() {
            return new ConnectorProviderVersion(1, 0);
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of(operation);
        }

        @Override
        public Optional<ConnectorConfigSchema<ProviderConfig>> configurationSchema() {
            return Optional.of(providerSchema());
        }

        @Override
        public CompletionStage<Void> start(ConnectorRuntimeContext context) {
            return ConnectorCompletionStages.completed();
        }

        @Override
        public CompletionStage<Void> start(ConnectorRuntimeContext context, ProviderConfig configuration) {
            startedWith = configuration;
            operation.runtimeContext = context;
            ConnectionResolutionRequest<FakeConnection> request = new ConnectionResolutionRequest<>(
                configuration.connection(), FakeConnection.class, ConnectorExecutionContext.empty());
            return context.connectionResolver().orElseThrow().resolve(request).thenAccept(ignored -> {
            });
        }

        @Override
        public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
            return ConnectorCompletionStages.completed();
        }
    }

    private static final class FakeQuery implements QueryOperation<String, OperationConfig, String> {
        private final AtomicInteger invocations = new AtomicInteger();
        private ConnectorRuntimeContext runtimeContext = ConnectorRuntimeContext.empty();
        private OperationConfig invokedWith;

        @Override
        public String id() {
            return "query";
        }

        @Override
        public Optional<ConnectorConfigSchema<OperationConfig>> configurationSchema() {
            return Optional.of(operationSchema());
        }

        @Override
        public CompletionStage<QueryOutcome<String>> query(QueryInvocation<String, OperationConfig, String> invocation) {
            invocations.incrementAndGet();
            invokedWith = invocation.configuration();
            return runtimeContext.secretResolver().orElseThrow().resolve(new SecretRef("query-token"))
                .thenApply(ignored -> new QueryOutcome.Found<>("found"));
        }
    }
}
