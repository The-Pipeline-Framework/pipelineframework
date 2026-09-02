package org.pipelineframework.query;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Uni;
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
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryObservationOrigin;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.QueryTokenUsage;
import org.pipelineframework.connector.TestConnectorBindingRegistries;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.mapper.Mapper;

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
        PipelineExecutionContextHolder.set(new PipelineExecutionContext(
            "tenant-1", "execution-1", "mail-pipeline", "contract-3", "release-9", 2,
            Optional.of("correlation-5"), Optional.of("trace-7")));
        operation.outcome = new QueryOutcome.Found<>(new Snapshot("customer-1", "LOW"));

        Snapshot output = support.queryOneToOne(descriptor(Map.of("index", "customers")),
            new Lookup("customer-1"), Snapshot.class).await().atMost(Duration.ofSeconds(2));

        assertEquals(new Snapshot("customer-1", "LOW"), output);
        assertEquals(new QueryConfig("customers"), operation.configuration);
        assertEquals(Snapshot.class, operation.outputType);
        assertEquals(1, operation.invocations.get());
        assertEquals(1, operation.providerStarts.get());
        ConnectorExecutionContext execution = operation.executionContext;
        assertEquals(Optional.of("tenant-1"), execution.tenantId());
        assertEquals(Optional.of("execution-1"), execution.executionId());
        assertEquals(Optional.of("mail-pipeline"), execution.pipelineId());
        assertEquals(Optional.of("contract-3"), execution.contractVersion());
        assertEquals(Optional.of("release-9"), execution.releaseVersion());
        assertEquals(Optional.of("LoadCustomer"), execution.stepId());
        assertEquals(Optional.of("correlation-5"), execution.correlationId());
        assertEquals(Optional.of("trace-7"), execution.traceId());
        assertEquals(ConnectorBindingName.of("lookup"),
            execution.invocationTarget().orElseThrow().bindingName());
        assertEquals(ConnectorProviderId.of("acme.lookup"),
            execution.invocationTarget().orElseThrow().operation().providerId());
    }

    @Test
    void preservesManagedExecutionContextAcrossAsynchronousDescriptorResolution() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext(
            "tenant-async", "execution-async", "mail-pipeline", "contract-3", "release-9", 2,
            Optional.of("correlation-5"), Optional.of("trace-7")));
        operation.outcome = new QueryOutcome.Found<>(new Snapshot("customer-1", "LOW"));
        ExecutorService descriptorLoader = Executors.newSingleThreadExecutor();

        try {
            Uni<QueryStepDescriptor> descriptor = Uni.createFrom()
                .item(() -> descriptor(Map.of("index", "customers")))
                .runSubscriptionOn(descriptorLoader);

            Snapshot output = support.queryOneToOne(descriptor, new Lookup("customer-1"), Snapshot.class)
                .await().atMost(Duration.ofSeconds(2));

            assertEquals(new Snapshot("customer-1", "LOW"), output);
            assertEquals(Optional.of("tenant-async"), operation.executionContext.tenantId());
            assertEquals(Optional.of("execution-async"), operation.executionContext.executionId());
        } finally {
            descriptorLoader.shutdownNow();
        }
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
    void capturesLiveObservationAndMarksSemanticReplayWithoutAnotherProviderCall() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "observed-execution", 3));
        QueryObservation observation = observation();
        operation.outcome = new QueryOutcome.Found<>(
            new Snapshot("customer-1", "MEDIUM"), Optional.of(observation));
        QueryStepDescriptor descriptor = descriptor(Map.of("index", "customers"));

        QueryOutcome.Found<?> live = assertInstanceOf(QueryOutcome.Found.class,
            support.queryOutcomeOneToOne(descriptor, new Lookup("customer-1"), Snapshot.class)
                .await().atMost(Duration.ofSeconds(2)));
        QueryStepSupport replayWithoutProvider = new QueryStepSupport(
            List.of(), List.of(captureStore), unavailableBindings());
        QueryOutcome.Found<?> replayed = assertInstanceOf(QueryOutcome.Found.class,
            replayWithoutProvider.queryOutcomeOneToOne(descriptor, new Lookup("customer-1"), Snapshot.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertEquals(QueryObservationOrigin.LIVE_PROVIDER, live.observation().orElseThrow().origin());
        assertEquals(QueryObservationOrigin.CAPTURE_REPLAY, replayed.observation().orElseThrow().origin());
        assertEquals(observation.tokenUsage(), replayed.observation().orElseThrow().tokenUsage());
        assertEquals(live.output(), replayed.output());
        assertEquals(1, operation.invocations.get());
    }

    @Test
    void mapsExternalRepresentationBeforeCanonicalCaptureAndReplay() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "mapped-execution", 3));
        operation.outcome = new QueryOutcome.Found<>(new Snapshot("customer-1", "MEDIUM"));
        QueryStepDescriptor descriptor = descriptor(Map.of("index", "customers"));
        AtomicInteger mappings = new AtomicInteger();
        Mapper<CanonicalSnapshot, Snapshot> mapper = new Mapper<>() {
            @Override
            public CanonicalSnapshot fromExternal(Snapshot external) {
                mappings.incrementAndGet();
                return new CanonicalSnapshot(external.customerId(), external.risk());
            }

            @Override
            public Snapshot toExternal(CanonicalSnapshot domain) {
                return new Snapshot(domain.customerId(), domain.risk());
            }
        };

        CanonicalSnapshot first = support.queryOneToOne(
                descriptor, new Lookup("customer-1"), CanonicalSnapshot.class, Snapshot.class, mapper)
            .await().atMost(Duration.ofSeconds(2));
        QueryStepSupport replayWithoutProvider = new QueryStepSupport(
            List.of(), List.of(captureStore), unavailableBindings());
        CanonicalSnapshot replayed = replayWithoutProvider.queryOneToOne(
                descriptor, new Lookup("customer-1"), CanonicalSnapshot.class, Snapshot.class, mapper)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(first, replayed);
        assertEquals(new CanonicalSnapshot("customer-1", "MEDIUM"), replayed);
        assertEquals(1, mappings.get());
        assertEquals(1, operation.invocations.get());
        assertEquals(Snapshot.class, operation.outputType);
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
    void exposesCapturedNotFoundAsAValidSemanticObservationWithoutChangingOrdinaryQueryBehavior() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 5));
        operation.outcome = new QueryOutcome.NotFound<>("customer-missing", Optional.of(observation()));
        QueryStepDescriptor descriptor = descriptor(Map.of("index", "customers"));

        QueryOutcome.NotFound<?> first = assertInstanceOf(QueryOutcome.NotFound.class,
            support.queryOutcomeOneToOne(descriptor, new Lookup("customer-1"), Snapshot.class)
                .await().atMost(Duration.ofSeconds(2)));
        QueryStepSupport replayWithoutProvider = new QueryStepSupport(
            List.of(), List.of(captureStore), unavailableBindings());
        QueryOutcome.NotFound<?> replayed = assertInstanceOf(QueryOutcome.NotFound.class,
            replayWithoutProvider.queryOutcomeOneToOne(descriptor, new Lookup("customer-1"), Snapshot.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertEquals("customer-missing", first.code());
        assertEquals(first.code(), replayed.code());
        assertEquals(QueryObservationOrigin.LIVE_PROVIDER, first.observation().orElseThrow().origin());
        assertEquals(QueryObservationOrigin.CAPTURE_REPLAY, replayed.observation().orElseThrow().origin());
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

    private static QueryObservation observation() {
        return QueryObservation.live(
            Optional.of(new QueryTokenUsage(
                OptionalLong.of(8), OptionalLong.of(5), OptionalLong.of(17))),
            Optional.of("provider-model"), Optional.of("stop"));
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
        return TestConnectorBindingRegistries.fromProviderSupplier(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("lookup"),
                ConnectorProviderId.of("acme.lookup"),
                1,
                ConnectorConfigurationDocument.empty())),
            new FakeProvider(operation),
            () -> new FakeProvider(operation));
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
        return TestConnectorBindingRegistries.fromProviderSupplier(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("zero"),
                ConnectorProviderId.of("acme.zero"),
                1,
                ConnectorConfigurationDocument.empty())),
            new ZeroConfigProvider(operation),
            () -> new ZeroConfigProvider(operation));
    }

    public record Lookup(String customerId) {
    }

    public record QueryConfig(String index) {
    }

    public record Snapshot(String customerId, String risk) {
    }

    public record CanonicalSnapshot(String customerId, String risk) {
    }

    public static final class FakeProvider implements ConnectorProvider<Void> {
        private final FakeQueryOperation operation;

        FakeProvider(FakeQueryOperation operation) {
            this.operation = operation;
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
        private final ZeroConfigQueryOperation operation;

        ZeroConfigProvider(ZeroConfigQueryOperation operation) {
            this.operation = operation;
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
        private ConnectorExecutionContext executionContext;
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
            executionContext = invocation.executionContext();
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
