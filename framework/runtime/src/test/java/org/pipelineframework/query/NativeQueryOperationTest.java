package org.pipelineframework.query;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeQueryOperationTest {
    private final InMemoryQueryCaptureStore captureStore = new InMemoryQueryCaptureStore();
    private final FakeQueryOperation operation = new FakeQueryOperation();
    private final ConnectorBindingRegistry bindings = bindings(operation);
    private final QueryStepSupport support = new QueryStepSupport(List.of(), List.of(captureStore), bindings);

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void bindsTypedConfigAndReturnsFoundOutput() {
        operation.outcome = new QueryOutcome.Found<>(new Snapshot("customer-1", "LOW"));

        Snapshot output = support.queryOneToOne(descriptor(Map.of("index", "customers")),
            new Lookup("customer-1"), Snapshot.class).await().atMost(Duration.ofSeconds(2));

        assertEquals(new Snapshot("customer-1", "LOW"), output);
        assertEquals(new QueryConfig("customers"), operation.configuration);
        assertEquals(Snapshot.class, operation.outputType);
        assertEquals(1, operation.invocations.get());
        assertEquals(1, operation.providerStarts.get());
    }

    @Test
    void captureReplayPrecedesProviderResolution() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 3));
        operation.outcome = new QueryOutcome.Found<>(new Snapshot("customer-1", "MEDIUM"));
        QueryStepDescriptor descriptor = descriptor(Map.of("index", "customers"));

        Snapshot first = support.queryOneToOne(descriptor, new Lookup("customer-1"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2));
        QueryStepSupport replayWithoutProvider = new QueryStepSupport(
            List.of(), List.of(captureStore), unavailableBindings());
        Snapshot replayed = replayWithoutProvider.queryOneToOne(
            descriptor, new Lookup("customer-1"), Snapshot.class).await().atMost(Duration.ofSeconds(2));

        assertEquals(first, replayed);
        assertEquals(1, operation.invocations.get());
    }

    @Test
    void capturesAndReplaysNotFoundAsTheTypedPipelineFailure() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 4));
        operation.outcome = new QueryOutcome.NotFound<>("customer-missing");
        QueryStepDescriptor descriptor = descriptor(Map.of("index", "customers"));

        QueryNotFoundException first = assertThrows(QueryNotFoundException.class, () -> support.queryOneToOne(
            descriptor, new Lookup("customer-1"), Snapshot.class).await().atMost(Duration.ofSeconds(2)));
        QueryStepSupport replayWithoutProvider = new QueryStepSupport(
            List.of(), List.of(captureStore), unavailableBindings());
        QueryNotFoundException replayed = assertThrows(QueryNotFoundException.class, () -> replayWithoutProvider
            .queryOneToOne(descriptor, new Lookup("customer-1"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2)));

        assertEquals("customer-missing", first.outcomeCode());
        assertEquals(first.outcomeCode(), replayed.outcomeCode());
        assertEquals(1, operation.invocations.get());
    }

    @Test
    void mapsEveryNonSuccessOutcomeToItsRetryClassification() {
        assertOutcome(
            new QueryOutcome.TemporarilyUnavailable<>("provider-busy"),
            QueryTemporarilyUnavailableException.class);
        assertOutcome(
            new QueryOutcome.AuthenticationRequired<>("authentication-required"),
            QueryAuthenticationRequiredException.class);
        assertOutcome(
            new QueryOutcome.TerminalFailure<>("invalid-query"),
            QueryTerminalFailureException.class);
    }

    @Test
    void invalidConfigurationFailsBeforeOperationInvocation() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> support.queryOneToOne(
            descriptor(Map.of("unknown", "value")), new Lookup("customer-1"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2)));

        assertEquals(0, operation.invocations.get());
        org.junit.jupiter.api.Assertions.assertTrue(failure.getMessage().contains("find.customer"));
    }

    @Test
    void executesSchemaLessQueryWithTheEmptyDocumentAndRejectsSuppliedConfiguration() {
        ZeroConfigQueryOperation zeroConfigOperation = new ZeroConfigQueryOperation();
        ConnectorBindingRegistry zeroConfigBindings = zeroConfigBindings(zeroConfigOperation);
        QueryStepSupport zeroConfigSupport = new QueryStepSupport(List.of(), List.of(), zeroConfigBindings);

        Snapshot output = zeroConfigSupport.queryOneToOne(
            zeroConfigDescriptor(Map.of()), new Lookup("customer-1"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(new Snapshot("customer-1", "ZERO"), output);
        assertEquals(ConnectorConfigurationDocument.empty(), zeroConfigOperation.configuration);
        assertEquals(1, zeroConfigOperation.invocations.get());

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> zeroConfigSupport
            .queryOneToOne(zeroConfigDescriptor(Map.of("unexpected", "value")),
                new Lookup("customer-2"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2)));
        assertTrue(failure.getMessage().contains("does not declare a configuration schema"));
        assertEquals(1, zeroConfigOperation.invocations.get());
    }

    @Test
    void providerBugsFailWithoutCapture() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 5));
        operation.returnNullStage = true;

        IllegalStateException nullStage = assertThrows(IllegalStateException.class, () -> support.queryOneToOne(
            descriptor(Map.of("index", "customers")), new Lookup("customer-1"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2)));
        assertInstanceOf(IllegalStateException.class, nullStage);

        operation.returnNullStage = false;
        operation.outcome = null;
        IllegalStateException nullOutcome = assertThrows(IllegalStateException.class, () -> support.queryOneToOne(
            descriptor(Map.of("index", "customers")), new Lookup("customer-2"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2)));
        org.junit.jupiter.api.Assertions.assertTrue(nullOutcome.getMessage().contains("null outcome"));
    }

    @Test
    void completionStageFailuresAndCancellationCrossTheRuntimeBoundaryWithoutWrapperLeakage() {
        RuntimeException original = new RuntimeException("provider-failed");
        operation.immediateFailure = original;
        RuntimeException immediate = assertThrows(RuntimeException.class, () -> support.queryOneToOne(
            descriptor(Map.of("index", "customers")), new Lookup("customer-1"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2)));
        assertSame(original, immediate);

        operation.immediateFailure = null;
        CompletableFuture<QueryOutcome<Snapshot>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new CompletionException(original));
        operation.stageOverride = failed;
        RuntimeException asynchronous = assertThrows(RuntimeException.class, () -> support.queryOneToOne(
            descriptor(Map.of("index", "customers")), new Lookup("customer-2"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2)));
        assertSame(original, asynchronous);

        CompletableFuture<QueryOutcome<Snapshot>> pending = new CompletableFuture<>();
        operation.stageOverride = pending;
        var cancellable = support.queryOneToOne(
            descriptor(Map.of("index", "customers")), new Lookup("customer-3"), Snapshot.class)
            .subscribe().with(ignored -> { }, ignored -> { });
        cancellable.cancel();
        assertTrue(pending.isCancelled());
    }

    private void assertOutcome(QueryOutcome<Snapshot> outcome, Class<? extends Throwable> expected) {
        operation.outcome = outcome;
        Throwable failure = assertThrows(expected, () -> support.queryOneToOne(
            descriptor(Map.of("index", "customers")), new Lookup("customer-1"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2)));
        assertInstanceOf(expected, failure);
    }

    private QueryStepDescriptor descriptor(Map<String, Object> configuration) {
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("acme.lookup"), "find.customer", ConnectorOperationKind.QUERY, 1);
        return QueryStepDescriptor.nativeQuery(
            "LoadCustomer",
            Lookup.class.getName(),
            Snapshot.class.getName(),
            "ONE_TO_ONE",
            new NativeQuerySelector(ConnectorBindingName.of("lookup"), identity, 1),
            configuration,
            FakeQueryOperation.CAPABILITIES,
            Optional.empty());
    }

    private QueryStepDescriptor zeroConfigDescriptor(Map<String, Object> configuration) {
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("acme.zero"), "find.zero", ConnectorOperationKind.QUERY, 1);
        return QueryStepDescriptor.nativeQuery(
            "LoadZeroConfigCustomer",
            Lookup.class.getName(),
            Snapshot.class.getName(),
            "ONE_TO_ONE",
            new NativeQuerySelector(ConnectorBindingName.of("zero"), identity, 1),
            configuration,
            QueryCapabilities.cacheable(),
            Optional.empty());
    }

    private static ConnectorBindingRegistry bindings(FakeQueryOperation operation) {
        FakeProvider.operation = operation;
        return ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("lookup"),
                ConnectorProviderId.of("acme.lookup"),
                1,
                ConnectorConfigurationDocument.empty())),
            List.of(new FakeProvider()));
    }

    private static ConnectorBindingRegistry unavailableBindings() {
        return ConnectorBindingRegistry.fromProvidersAllowingUnavailable(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("lookup"),
                ConnectorProviderId.of("acme.lookup"),
                1,
                ConnectorConfigurationDocument.empty())),
            List.of());
    }

    private static ConnectorBindingRegistry zeroConfigBindings(ZeroConfigQueryOperation operation) {
        ZeroConfigProvider.operation = operation;
        return ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("zero"),
                ConnectorProviderId.of("acme.zero"),
                1,
                ConnectorConfigurationDocument.empty())),
            List.of(new ZeroConfigProvider()));
    }

    public record Lookup(String customerId) {
    }

    public record QueryConfig(String index) {
    }

    public record Snapshot(String customerId, String risk) {
    }

    public static final class FakeProvider implements ConnectorProvider<Void> {
        private static FakeQueryOperation operation;

        public FakeProvider() {
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.lookup");
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
        public CompletionStage<Void> start(ConnectorRuntimeContext context) {
            operation.providerStarts.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public static final class ZeroConfigProvider implements ConnectorProvider<Void> {
        private static ZeroConfigQueryOperation operation;

        public ZeroConfigProvider() {
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.zero");
        }

        @Override
        public ConnectorProviderVersion version() {
            return new ConnectorProviderVersion(1, 0);
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of(operation);
        }
    }

    private static final class ZeroConfigQueryOperation
        implements QueryOperation<Lookup, ConnectorConfigurationDocument, Snapshot> {
        private final AtomicInteger invocations = new AtomicInteger();
        private ConnectorConfigurationDocument configuration;

        @Override
        public String id() {
            return "find.zero";
        }

        @Override
        public QueryCapabilities capabilities() {
            return QueryCapabilities.cacheable();
        }

        @Override
        public CompletionStage<QueryOutcome<Snapshot>> query(
            QueryInvocation<Lookup, ConnectorConfigurationDocument, Snapshot> invocation
        ) {
            invocations.incrementAndGet();
            configuration = invocation.configuration();
            return CompletableFuture.completedFuture(
                new QueryOutcome.Found<>(new Snapshot(invocation.input().customerId(), "ZERO")));
        }
    }

    private static final class FakeQueryOperation implements QueryOperation<Lookup, QueryConfig, Snapshot> {
        private static final ConnectorConfigSchema<QueryConfig> SCHEMA =
            ConnectorConfigSchema.record(QueryConfig.class, "acme.lookup.find.customer", 1);
        private static final QueryCapabilities CAPABILITIES = QueryCapabilities.cacheable();

        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicInteger providerStarts = new AtomicInteger();
        private QueryOutcome<Snapshot> outcome = new QueryOutcome.Found<>(new Snapshot("default", "LOW"));
        private QueryConfig configuration;
        private Class<?> outputType;
        private boolean returnNullStage;
        private RuntimeException immediateFailure;
        private CompletionStage<QueryOutcome<Snapshot>> stageOverride;

        @Override
        public String id() {
            return "find.customer";
        }

        @Override
        public QueryCapabilities capabilities() {
            return CAPABILITIES;
        }

        @Override
        public Optional<ConnectorConfigSchema<QueryConfig>> configurationSchema() {
            return Optional.of(SCHEMA);
        }

        @Override
        public CompletionStage<QueryOutcome<Snapshot>> query(QueryInvocation<Lookup, QueryConfig, Snapshot> invocation) {
            invocations.incrementAndGet();
            configuration = invocation.configuration();
            outputType = invocation.outputType();
            if (immediateFailure != null) {
                throw immediateFailure;
            }
            if (returnNullStage) {
                return null;
            }
            if (stageOverride != null) {
                CompletionStage<QueryOutcome<Snapshot>> result = stageOverride;
                stageOverride = null;
                return result;
            }
            return CompletableFuture.completedFuture(outcome);
        }
    }
}
