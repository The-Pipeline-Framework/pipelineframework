package org.pipelineframework;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.cache.CacheKeyTarget;
import org.pipelineframework.cache.CachePolicyViolation;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.cache.PipelineCacheReader;
import org.pipelineframework.cache.PipelineCacheWriter;
import org.pipelineframework.cache.QueryNotFoundCacheEntry;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.QueryCacheability;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import org.pipelineframework.query.NativeQuerySelector;
import org.pipelineframework.query.ProviderQueryStep;
import org.pipelineframework.query.QueryCacheRequirements;
import org.pipelineframework.query.QueryNotFoundException;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProviderQueryCacheConformanceTest {

    @Test
    void preferAndRequireHitsArePipelineReplayWithoutLiveObservation() {
        QueryOutput cached = new QueryOutput("cached");
        RecordingCache cache = new RecordingCache(Map.of("2:v1:key", cached));
        GeneratedLikeQueryStep preferStep = step(QueryCapabilities.cacheable(), Optional.empty(), false);
        GeneratedLikeQueryStep requireStep = step(QueryCapabilities.cacheable(), Optional.empty(), false);

        assertEquals(cached, run(preferStep, cache, "prefer-cache", Optional.empty()));
        assertEquals(cached, run(requireStep, cache, "require-cache", Optional.empty()));
        assertEquals(0, preferStep.calls.get());
        assertEquals(0, requireStep.calls.get());
        assertEquals(2, cache.getCalls.get());
    }

    @Test
    void preferMissAndCacheOnlyPerformLiveObservation() {
        RecordingCache cache = new RecordingCache(Map.of());
        GeneratedLikeQueryStep prefer = step(QueryCapabilities.cacheable(), Optional.empty(), false);
        GeneratedLikeQueryStep cacheOnly = step(QueryCapabilities.cacheable(), Optional.empty(), false);

        assertEquals(new QueryOutput("live-input"), run(prefer, cache, "prefer-cache", Optional.empty()));
        assertEquals(new QueryOutput("live-input"), run(cacheOnly, cache, "cache-only", Optional.empty()));
        assertEquals(1, prefer.calls.get());
        assertEquals(1, cacheOnly.calls.get());
        assertEquals(1, cache.getCalls.get());
    }

    @Test
    void bypassPerformsNoGenericCacheIo() {
        RecordingCache cache = new RecordingCache(Map.of("2:v1:key", new QueryOutput("old")));
        GeneratedLikeQueryStep step = step(QueryCapabilities.conservative(), Optional.empty(), false);

        assertEquals(new QueryOutput("live-input"), run(step, cache, "bypass-cache", Optional.empty()));
        assertEquals(1, step.calls.get());
        assertEquals(0, cache.getCalls.get());
        assertEquals(0, cache.putCalls.get());
    }

    @Test
    void rejectsSkipIfPresentAndLiveOnlyReuseBeforeQueryExecution() {
        RecordingCache cache = new RecordingCache(Map.of("2:v1:key", new QueryOutput("old")));
        GeneratedLikeQueryStep cacheable = step(QueryCapabilities.cacheable(), Optional.empty(), false);
        GeneratedLikeQueryStep liveOnly = step(QueryCapabilities.conservative(), Optional.empty(), false);

        CachePolicyViolation skip = assertThrows(CachePolicyViolation.class,
            () -> run(cacheable, cache, "skip-if-present", Optional.empty()));
        CachePolicyViolation live = assertThrows(CachePolicyViolation.class,
            () -> run(liveOnly, cache, "prefer-cache", Optional.empty()));

        assertTrue(skip.getMessage().contains("neither replays the existing value nor records a miss"));
        assertTrue(live.getMessage().contains("LIVE_ONLY"));
        assertEquals(0, cacheable.calls.get());
        assertEquals(0, liveOnly.calls.get());
        assertEquals(0, cache.getCalls.get());
    }

    @Test
    void positiveTtlIsOptionalUnlessProviderDeclaresAMaximum() {
        RecordingCache cache = new RecordingCache(Map.of());
        GeneratedLikeQueryStep unbounded = step(QueryCapabilities.cacheable(), Optional.empty(), false);
        QueryCapabilities boundedCapabilities = new QueryCapabilities(
            QueryCacheability.CACHEABLE, Optional.of(Duration.ofMinutes(5)), Optional.empty());
        GeneratedLikeQueryStep bounded = step(boundedCapabilities, Optional.empty(), false);

        assertEquals(new QueryOutput("live-input"), run(unbounded, cache, "prefer-cache", Optional.empty()));
        assertThrows(CachePolicyViolation.class,
            () -> run(bounded, cache, "prefer-cache", Optional.empty()));
        assertThrows(CachePolicyViolation.class,
            () -> run(bounded, cache, "prefer-cache", Optional.of(Duration.ofMinutes(10))));
        assertEquals(new QueryOutput("live-input"),
            run(bounded, cache, "prefer-cache", Optional.of(Duration.ofMinutes(5))));
    }

    @Test
    @SuppressWarnings("unchecked")
    void absentCacheSubsystemBypassesPositiveMaximumAgeValidation() {
        QueryCapabilities boundedCapabilities = new QueryCapabilities(
            QueryCacheability.CACHEABLE, Optional.of(Duration.ofMinutes(5)), Optional.empty());
        GeneratedLikeQueryStep bounded = step(boundedCapabilities, Optional.empty(), false);

        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            bounded, Uni.createFrom().item(new QueryInput("input")));

        assertEquals(new QueryOutput("live-input"), ((Uni<QueryOutput>) result).await().indefinitely());
        assertEquals(1, bounded.calls.get());
    }

    @Test
    void negativeCacheFailureReportsMissingWriterAsPolicyViolation() {
        QueryCapabilities capabilities = new QueryCapabilities(
            QueryCacheability.CACHEABLE, Optional.empty(), Optional.of(Duration.ofSeconds(30)));
        GeneratedLikeQueryStep step = step(capabilities, Optional.of(Duration.ofSeconds(20)), true);
        RecordingCache reader = new RecordingCache(Map.of());
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            reader,
            Optional.empty(),
            List.of(new QueryCacheKeyStrategy()),
            "cache-only",
            Optional.empty());

        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item(new QueryInput("input")),
            false,
            1,
            support,
            PipelineContext.fromHeaders("v1", "", "cache-only"),
            new AwaitExecutionContext("tenant", "execution", 1));
        CachePolicyViolation failure = assertThrows(CachePolicyViolation.class,
            () -> ((Uni<?>) result).await().indefinitely());

        assertTrue(failure.getMessage().contains("acme.lookup"));
        assertTrue(failure.getMessage().contains("bounded writes"));
    }

    @Test
    void boundedNegativeMarkerReplaysAndIsWrittenOnlyForOptedInLiveMisses() {
        Duration negativeTtl = Duration.ofSeconds(20);
        QueryCapabilities capabilities = new QueryCapabilities(
            QueryCacheability.CACHEABLE, Optional.empty(), Optional.of(Duration.ofSeconds(30)));
        RecordingCache cache = new RecordingCache(Map.of());
        GeneratedLikeQueryStep live = step(capabilities, Optional.of(negativeTtl), true);

        QueryNotFoundException first = assertThrows(QueryNotFoundException.class,
            () -> run(live, cache, "prefer-cache", Optional.empty()));
        assertEquals("missing", first.outcomeCode());
        assertEquals(1, live.calls.get());
        assertEquals(negativeTtl, cache.lastTtl);
        assertEquals(new QueryNotFoundCacheEntry("missing"), cache.values.get("2:v1:key"));

        GeneratedLikeQueryStep replay = step(capabilities, Optional.of(negativeTtl), true);
        QueryNotFoundException replayed = assertThrows(QueryNotFoundException.class,
            () -> run(replay, cache, "require-cache", Optional.empty()));
        assertEquals("missing", replayed.outcomeCode());
        assertEquals(0, replay.calls.get());
    }

    @Test
    void cacheOnlyWritesABoundedNegativeMarkerWhileBypassNeverDoes() {
        Duration negativeTtl = Duration.ofSeconds(20);
        QueryCapabilities capabilities = new QueryCapabilities(
            QueryCacheability.CACHEABLE, Optional.empty(), Optional.of(Duration.ofSeconds(30)));
        RecordingCache cacheOnlyCache = new RecordingCache(Map.of());
        GeneratedLikeQueryStep cacheOnly = step(capabilities, Optional.of(negativeTtl), true);

        assertThrows(QueryNotFoundException.class,
            () -> run(cacheOnly, cacheOnlyCache, "cache-only", Optional.empty()));
        assertEquals(0, cacheOnlyCache.getCalls.get());
        assertEquals(1, cacheOnlyCache.putCalls.get());
        assertEquals(new QueryNotFoundCacheEntry("missing"), cacheOnlyCache.values.get("2:v1:key"));
        assertEquals(CacheStatus.BYPASS, cacheOnly.observedCacheStatus);

        RecordingCache bypassCache = new RecordingCache(Map.of());
        GeneratedLikeQueryStep bypass = step(capabilities, Optional.of(negativeTtl), true);
        assertThrows(QueryNotFoundException.class,
            () -> run(bypass, bypassCache, "bypass-cache", Optional.empty()));
        assertEquals(0, bypassCache.getCalls.get());
        assertEquals(0, bypassCache.putCalls.get());
    }

    @Test
    void negativeQueryMarkerCannotCrossAnOrdinaryPipelineOutputBoundary() {
        RecordingCache cache = new RecordingCache(Map.of("2:v1:key", new QueryNotFoundCacheEntry("missing")));
        OrdinaryStep ordinary = new OrdinaryStep();

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> run(ordinary, cache, "prefer-cache", Optional.empty()));

        assertTrue(failure.getMessage().contains("cannot be returned as ordinary pipeline output"), failure.getMessage());
        assertEquals(0, ordinary.calls.get());
    }

    @Test
    void resolvesCacheThenCaptureThenLiveProviderWithUnavailableReplaySupport() {
        RecordingProviderOperation operation = new RecordingProviderOperation();
        InMemoryQueryCaptureStore captures = new InMemoryQueryCaptureStore();
        QueryStepDescriptor descriptor = providerDescriptor();
        ConnectorBindingRegistry seedBindings = providerBindings(operation);
        try {
            PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "captured", 2));
            QueryOutput captured = new QueryStepSupport(List.of(), List.of(captures), seedBindings)
                .queryOneToOne(descriptor, new QueryInput("input"), QueryOutput.class)
                .await().atMost(Duration.ofSeconds(2));
            assertEquals(new QueryOutput("provider-input"), captured);
            PipelineExecutionContextHolder.clear();
            seedBindings.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
            operation.invocations.set(0);
            operation.starts.set(0);

            ConnectorBindingRegistry unavailable = unavailableProviderBindings();
            GeneratedProviderBackedQueryStep replayStep =
                new GeneratedProviderBackedQueryStep(new QueryStepSupport(List.of(), List.of(captures), unavailable), descriptor);
            RecordingCache cacheHit = new RecordingCache(Map.of("2:v1:key", new QueryOutput("cache")));
            assertEquals(new QueryOutput("cache"), run(replayStep, cacheHit, "prefer-cache", Optional.empty()));
            assertEquals(0, replayStep.calls.get());

            RecordingCache cacheMiss = new RecordingCache(Map.of());
            assertEquals(new QueryOutput("provider-input"),
                run(replayStep, cacheMiss, "prefer-cache", Optional.empty(),
                    new AwaitExecutionContext("tenant", "captured", 2)));
            assertEquals(1, replayStep.calls.get());
            assertEquals(0, operation.invocations.get());
            assertEquals(0, operation.starts.get());

            ConnectorBindingRegistry liveBindings = providerBindings(operation);
            try {
                GeneratedProviderBackedQueryStep liveStep = new GeneratedProviderBackedQueryStep(
                    new QueryStepSupport(List.of(), List.of(captures), liveBindings), descriptor);
                assertEquals(new QueryOutput("provider-input"),
                    run(liveStep, new RecordingCache(Map.of()), "prefer-cache", Optional.empty(),
                        new AwaitExecutionContext("tenant", "live", 2)));
                assertEquals(1, liveStep.calls.get());
                assertEquals(1, operation.invocations.get());
                assertEquals(1, operation.starts.get());
            } finally {
                liveBindings.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
            }
        } finally {
            seedBindings.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
            PipelineExecutionContextHolder.clear();
        }
    }

    private static GeneratedLikeQueryStep step(
        QueryCapabilities capabilities,
        Optional<Duration> negativeCacheTtl,
        boolean notFound
    ) {
        return new GeneratedLikeQueryStep(new QueryCacheRequirements(
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("acme.lookup"), "find.customer", ConnectorOperationKind.QUERY, 1),
            1,
            capabilities,
            negativeCacheTtl),
            notFound);
    }

    private static QueryStepDescriptor providerDescriptor() {
        return QueryStepDescriptor.nativeQuery(
            "LoadProviderObservation",
            QueryInput.class.getName(),
            QueryOutput.class.getName(),
            "ONE_TO_ONE",
            new NativeQuerySelector(ConnectorBindingName.of("lookup"), providerIdentity(), 1),
            Map.of(),
            QueryCapabilities.cacheable(),
            Optional.empty());
    }

    private static ConnectorOperationIdentity providerIdentity() {
        return new ConnectorOperationIdentity(
            ConnectorProviderId.of("acme.replay"), "find.observation", ConnectorOperationKind.QUERY, 1);
    }

    private static ConnectorBindingDefinition providerBinding() {
        return new ConnectorBindingDefinition(
            ConnectorBindingName.of("lookup"),
            ConnectorProviderId.of("acme.replay"),
            1,
            ConnectorConfigurationDocument.empty());
    }

    private static ConnectorBindingRegistry providerBindings(RecordingProviderOperation operation) {
        RecordingProvider.operation = operation;
        return ConnectorBindingRegistry.fromProviders(List.of(providerBinding()), List.of(new RecordingProvider()));
    }

    private static ConnectorBindingRegistry unavailableProviderBindings() {
        return ConnectorBindingRegistry.fromProvidersAllowingUnavailable(List.of(providerBinding()), List.of());
    }

    private static QueryOutput run(
        StepOneToOne<QueryInput, QueryOutput> step,
        RecordingCache cache,
        String policy,
        Optional<Duration> configuredTtl
    ) {
        return run(step, cache, policy, configuredTtl, null);
    }

    private static QueryOutput run(
        StepOneToOne<QueryInput, QueryOutput> step,
        RecordingCache cache,
        String policy,
        Optional<Duration> configuredTtl,
        AwaitExecutionContext executionContext
    ) {
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            cache,
            Optional.of(cache),
            List.of(new QueryCacheKeyStrategy()),
            policy,
            configuredTtl);
        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item(new QueryInput("input")),
            false,
            1,
            support,
            PipelineContext.fromHeaders("v1", "", policy),
            executionContext);
        return ((Uni<QueryOutput>) result).await().indefinitely();
    }

    record QueryInput(String id) {
    }

    record QueryOutput(String observation) {
    }

    static final class OrdinaryStep extends ConfigurableStep
        implements StepOneToOne<QueryInput, QueryOutput>, CacheKeyTarget {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public Uni<QueryOutput> applyOneToOne(QueryInput input) {
            calls.incrementAndGet();
            return Uni.createFrom().item(new QueryOutput("ordinary-" + input.id()));
        }

        @Override
        public Class<?> cacheKeyTargetType() {
            return QueryOutput.class;
        }
    }

    static final class GeneratedLikeQueryStep extends ConfigurableStep
        implements StepOneToOne<QueryInput, QueryOutput>, ProviderQueryStep, CacheKeyTarget {
        private final QueryCacheRequirements requirements;
        private final boolean notFound;
        private final AtomicInteger calls = new AtomicInteger();
        private CacheStatus observedCacheStatus;

        GeneratedLikeQueryStep(QueryCacheRequirements requirements, boolean notFound) {
            this.requirements = requirements;
            this.notFound = notFound;
        }

        @Override
        public Uni<QueryOutput> applyOneToOne(QueryInput input) {
            calls.incrementAndGet();
            observedCacheStatus = PipelineCacheStatusHolder.get();
            return notFound
                ? Uni.createFrom().failure(new QueryNotFoundException("missing"))
                : Uni.createFrom().item(new QueryOutput("live-" + input.id()));
        }

        @Override
        public QueryCacheRequirements queryCacheRequirements() {
            return requirements;
        }

        @Override
        public Class<?> cacheKeyTargetType() {
            return QueryOutput.class;
        }
    }

    static final class GeneratedProviderBackedQueryStep extends ConfigurableStep
        implements StepOneToOne<QueryInput, QueryOutput>, ProviderQueryStep, CacheKeyTarget {
        private final QueryStepSupport support;
        private final QueryStepDescriptor descriptor;
        private final AtomicInteger calls = new AtomicInteger();

        GeneratedProviderBackedQueryStep(QueryStepSupport support, QueryStepDescriptor descriptor) {
            this.support = support;
            this.descriptor = descriptor;
        }

        @Override
        public Uni<QueryOutput> applyOneToOne(QueryInput input) {
            calls.incrementAndGet();
            return support.queryOneToOne(descriptor, input, QueryOutput.class);
        }

        @Override
        public QueryCacheRequirements queryCacheRequirements() {
            return new QueryCacheRequirements(
                providerIdentity(), 1, QueryCapabilities.cacheable(), Optional.empty());
        }

        @Override
        public Class<?> cacheKeyTargetType() {
            return QueryOutput.class;
        }
    }

    public static final class RecordingProvider implements ConnectorProvider<Void> {
        private static RecordingProviderOperation operation;

        public RecordingProvider() {
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.replay");
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
            operation.starts.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    static final class RecordingProviderOperation
        implements QueryOperation<QueryInput, ConnectorConfigurationDocument, QueryOutput> {
        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicInteger starts = new AtomicInteger();

        @Override
        public String id() {
            return "find.observation";
        }

        @Override
        public QueryCapabilities capabilities() {
            return QueryCapabilities.cacheable();
        }

        @Override
        public CompletionStage<QueryOutcome<QueryOutput>> query(
            QueryInvocation<QueryInput, ConnectorConfigurationDocument, QueryOutput> invocation
        ) {
            invocations.incrementAndGet();
            return CompletableFuture.completedFuture(
                new QueryOutcome.Found<>(new QueryOutput("provider-" + invocation.input().id())));
        }
    }

    static final class QueryCacheKeyStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of("key");
        }

        @Override
        public boolean supportsTarget(Class<?> targetType) {
            return targetType == QueryOutput.class;
        }
    }

    static final class RecordingCache implements PipelineCacheReader, PipelineCacheWriter {
        private final Map<String, Object> values = new ConcurrentHashMap<>();
        private final AtomicInteger getCalls = new AtomicInteger();
        private final AtomicInteger putCalls = new AtomicInteger();
        private Duration lastTtl = Duration.ZERO;

        RecordingCache(Map<String, Object> initialValues) {
            values.putAll(initialValues);
        }

        @Override
        public Uni<Optional<Object>> get(String key) {
            getCalls.incrementAndGet();
            return Uni.createFrom().item(Optional.ofNullable(values.get(key)));
        }

        @Override
        public Uni<Boolean> exists(String key) {
            return Uni.createFrom().item(values.containsKey(key));
        }

        @Override
        public Uni<Void> put(String key, Object value, Duration ttl) {
            putCalls.incrementAndGet();
            values.put(key, value);
            lastTtl = ttl;
            return Uni.createFrom().voidItem();
        }
    }
}
