package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.vertx.core.runtime.context.VertxContextSafetyToggle;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Multi;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.hibernate.reactive.mutiny.Mutiny;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.pipelineframework.connector.BlockingOperation;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.QueryStream;
import org.pipelineframework.connector.StreamingQueryOperation;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepDescriptorFactory;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;

@QuarkusTest
@QuarkusTestResource(
    value = HibernateReactiveQueryConnectorIT.PipelineConfiguration.class,
    restrictToAnnotatedClass = true)
class HibernateReactiveQueryConnectorIT {
    @Inject
    HibernateReactiveQueryConnector connector;

    @Inject
    Mutiny.SessionFactory sessionFactory;

    @Inject
    ConnectorBindingRegistry bindings;

    @Inject
    QueryStepDescriptorFactory descriptorFactory;

    @Inject
    Vertx vertx;

    @AfterEach
    void clearExecutionContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void findsAndProjectsWithPredicateBindingOnTheReactiveContext() throws Exception {
        replaceWith(new ReactiveCustomerRiskEntity("customer-1", "HIGH", 91, "ACTIVE"));
        QueryOperation<Object, JpaFindOneConfiguration, Object> operation = operation();
        AtomicBoolean mappedOnEventLoop = new AtomicBoolean();

        QueryOutcome<Object> outcome = onVertxContext(() -> operation.query(new QueryInvocation<>(
            new CustomerRiskLookup("customer-1", 80),
            configuration(Map.of(
                "customerId", new JpaPredicate("eq", List.of("input.customerId")),
                "score", new JpaPredicate("gte", List.of("input.minimumScore")),
                "status", new JpaPredicate("eq", List.of("ACTIVE")))),
            cast(CustomerRiskFacts.class),
            ConnectorExecutionContext.empty(),
            Optional.empty(),
            Optional.of(external -> {
                Context context = Vertx.currentContext();
                mappedOnEventLoop.set(context != null && context.isEventLoopContext());
                return external;
            }))));

        assertEquals(new CustomerRiskFacts("customer-1", "HIGH", 91),
            assertInstanceOf(QueryOutcome.Found.class, outcome).output());
        assertTrue(mappedOnEventLoop.get());
        assertFalse(operation instanceof BlockingOperation);
    }

    @Test
    void returnsNotFoundAndMultipleResultOutcomes() throws Exception {
        replaceWith();
        QueryOutcome<Object> missing = invoke(new CustomerRiskLookup("missing", 0), baseConfiguration());
        assertInstanceOf(QueryOutcome.NotFound.class, missing);

        replaceWith(
            new ReactiveCustomerRiskEntity("duplicate", "HIGH", 91, "ACTIVE"),
            new ReactiveCustomerRiskEntity("duplicate", "MEDIUM", 72, "ACTIVE"));
        QueryOutcome<Object> multiple = invoke(new CustomerRiskLookup("duplicate", 0), baseConfiguration());
        assertEquals("multiple-results", assertInstanceOf(QueryOutcome.TerminalFailure.class, multiple).code());
    }

    @Test
    void returnsTerminalFailureWhenTheLocalMapperFails() throws Exception {
        replaceWith(new ReactiveCustomerRiskEntity("mapper-failure", "HIGH", 91, "ACTIVE"));

        QueryOutcome<Object> outcome = onVertxContext(() -> operation().query(new QueryInvocation<>(
            new CustomerRiskLookup("mapper-failure", 0),
            baseConfiguration(),
            cast(CustomerRiskFacts.class),
            ConnectorExecutionContext.empty(),
            Optional.empty(),
            Optional.of(ignored -> { throw new IllegalStateException("mapper-failed"); }))));

        assertEquals("hibernate-reactive-query-failed",
            assertInstanceOf(QueryOutcome.TerminalFailure.class, outcome).code());
    }

    @Test
    void generatedPipelineInvocationUsesTheReactiveProvider() throws Exception {
        replaceWith(new ReactiveCustomerRiskEntity("generated", "CRITICAL", 99, "ACTIVE"));
        QueryStepDescriptor descriptor = descriptorFactory.descriptor(
            "LoadReactiveCustomerRisk",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName()).await().atMost(java.time.Duration.ofSeconds(10));

        CustomerRiskFacts output = onVertxContext(() -> new QueryStepSupport(List.of(), List.of(), bindings)
            .queryOneToOne(descriptor, new CustomerRiskLookup("generated", 0), CustomerRiskFacts.class)
            .subscribeAsCompletionStage());

        assertEquals(new CustomerRiskFacts("generated", "CRITICAL", 99), output);
    }

    @Test
    void findManyStreamsOrderedProjectedRowsOnTheReactiveContext() throws Exception {
        replaceWith(
            new ReactiveCustomerRiskEntity("many", "HIGH", 91, "ACTIVE"),
            new ReactiveCustomerRiskEntity("many", "LOW", 40, "ACTIVE"),
            new ReactiveCustomerRiskEntity("many", "MEDIUM", 72, "ACTIVE"));
        AtomicBoolean mappedOnEventLoop = new AtomicBoolean();
        StreamingQueryOperation<Object, JpaFindManyConfiguration, Object> operation = streamingOperation();

        List<Object> rows = onVertxContext(() -> {
            QueryStream<Object> stream = operation.query(new QueryInvocation<>(
                new CustomerRiskLookup("many", 0),
                manyConfiguration(Optional.empty()),
                cast(CustomerRiskFacts.class),
                ConnectorExecutionContext.empty(),
                Optional.empty(),
                Optional.of(external -> {
                    Context context = Vertx.currentContext();
                    mappedOnEventLoop.set(context != null && context.isEventLoopContext());
                    return external;
                })));
            return Multi.createFrom().publisher(stream.rows()).collect().asList().subscribeAsCompletionStage()
                .thenCompose(found -> stream.termination().thenApply(ignored -> found));
        });

        assertEquals(List.of(
            new CustomerRiskFacts("many", "LOW", 40),
            new CustomerRiskFacts("many", "MEDIUM", 72),
            new CustomerRiskFacts("many", "HIGH", 91)), rows);
        assertTrue(mappedOnEventLoop.get());
        assertFalse(operation instanceof BlockingOperation);
    }

    @Test
    void generatedFindManyInvocationUsesExistingOneToManyRuntime() throws Exception {
        replaceWith(
            new ReactiveCustomerRiskEntity("generated-many", "HIGH", 91, "ACTIVE"),
            new ReactiveCustomerRiskEntity("generated-many", "LOW", 40, "ACTIVE"));
        QueryStepDescriptor descriptor = descriptorFactory.descriptor(
            "LoadReactiveCustomerRisks",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName()).await().atMost(java.time.Duration.ofSeconds(10));

        List<CustomerRiskFacts> output = onVertxContext(() -> new QueryStepSupport(List.of(), List.of(), bindings)
            .queryOneToMany(descriptor, new CustomerRiskLookup("generated-many", 0), CustomerRiskFacts.class)
            .collect().asList().subscribeAsCompletionStage());

        assertEquals(List.of(
            new CustomerRiskFacts("generated-many", "LOW", 40),
            new CustomerRiskFacts("generated-many", "HIGH", 91)), output);
    }

    @Test
    void generatedFindManyMapsExternalRepresentationsPerRow() throws Exception {
        replaceWith(
            new ReactiveCustomerRiskEntity("mapped-many", "encoded:HIGH", 91, "ACTIVE"),
            new ReactiveCustomerRiskEntity("mapped-many", "encoded:LOW", 40, "ACTIVE"));
        QueryStepDescriptor descriptor = descriptorFactory.descriptor(
            "LoadReactiveCustomerRisks",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName()).await().atMost(java.time.Duration.ofSeconds(10));
        Mapper<CustomerRiskFacts, ReactiveCustomerRiskEntity> mapper = new ReactiveCustomerRiskMapper(false);

        List<CustomerRiskFacts> output = onVertxContext(() -> new QueryStepSupport(List.of(), List.of(), bindings)
            .queryOneToMany(
                descriptor,
                new CustomerRiskLookup("mapped-many", 0),
                CustomerRiskFacts.class,
                ReactiveCustomerRiskEntity.class,
                mapper)
            .collect().asList().subscribeAsCompletionStage());

        assertEquals(List.of(
            new CustomerRiskFacts("mapped-many", "LOW", 40),
            new CustomerRiskFacts("mapped-many", "HIGH", 91)), output);
    }

    @Test
    void streamingCaptureReplaysWithoutAnotherReactiveQuery() throws Exception {
        replaceWith(
            new ReactiveCustomerRiskEntity("captured-many", "HIGH", 91, "ACTIVE"),
            new ReactiveCustomerRiskEntity("captured-many", "LOW", 40, "ACTIVE"));
        QueryStepDescriptor descriptor = descriptorFactory.descriptor(
            "LoadReactiveCustomerRisks",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName()).await().atMost(java.time.Duration.ofSeconds(10));
        QueryStepSupport support = new QueryStepSupport(
            List.of(), List.of(new InMemoryQueryCaptureStore()), bindings);
        PipelineExecutionContext execution = new PipelineExecutionContext("tenant", "reactive-capture", 4);

        List<CustomerRiskFacts> first = onVertxContext(() -> withExecutionContext(execution, () -> support
            .queryOneToMany(descriptor, new CustomerRiskLookup("captured-many", 0), CustomerRiskFacts.class)
            .collect().asList().subscribeAsCompletionStage()));
        replaceWith(new ReactiveCustomerRiskEntity("captured-many", "CHANGED", 1, "ACTIVE"));
        List<CustomerRiskFacts> replay = onVertxContext(() -> withExecutionContext(execution, () -> support
            .queryOneToMany(descriptor, new CustomerRiskLookup("captured-many", 0), CustomerRiskFacts.class)
            .collect().asList().subscribeAsCompletionStage()));

        assertEquals(first, replay);
        assertEquals(List.of(
            new CustomerRiskFacts("captured-many", "LOW", 40),
            new CustomerRiskFacts("captured-many", "HIGH", 91)), replay);
    }

    @Test
    void partialReactiveMappingFailureAbortsCaptureBeforeRetry() throws Exception {
        replaceWith(
            new ReactiveCustomerRiskEntity("reactive-partial", "encoded:LOW", 40, "ACTIVE"),
            new ReactiveCustomerRiskEntity("reactive-partial", "encoded:MEDIUM", 72, "ACTIVE"));
        QueryStepDescriptor descriptor = descriptorFactory.descriptor(
            "LoadReactiveCustomerRisks",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName()).await().atMost(java.time.Duration.ofSeconds(10));
        QueryStepSupport support = new QueryStepSupport(
            List.of(), List.of(new InMemoryQueryCaptureStore()), bindings);
        PipelineExecutionContext execution = new PipelineExecutionContext("tenant", "reactive-partial", 4);
        Mapper<CustomerRiskFacts, ReactiveCustomerRiskEntity> mapper = new ReactiveCustomerRiskMapper(true);

        assertInstanceOf(ExecutionException.class, org.junit.jupiter.api.Assertions.assertThrows(
            ExecutionException.class,
            () -> onVertxContext(() -> withExecutionContext(execution, () -> support.queryOneToMany(
                    descriptor,
                    new CustomerRiskLookup("reactive-partial", 0),
                    CustomerRiskFacts.class,
                    ReactiveCustomerRiskEntity.class,
                    mapper)
                .collect().asList().subscribeAsCompletionStage()))));

        replaceWith(
            new ReactiveCustomerRiskEntity("reactive-partial", "encoded:LOW", 40, "ACTIVE"),
            new ReactiveCustomerRiskEntity("reactive-partial", "encoded:MEDIUM", 72, "ACTIVE"),
            new ReactiveCustomerRiskEntity("reactive-partial", "encoded:HIGH", 91, "ACTIVE"));
        List<CustomerRiskFacts> retry = onVertxContext(() -> withExecutionContext(execution, () -> support
            .queryOneToMany(
                descriptor,
                new CustomerRiskLookup("reactive-partial", 0),
                CustomerRiskFacts.class,
                ReactiveCustomerRiskEntity.class,
                mapper)
            .collect().asList().subscribeAsCompletionStage()));

        assertEquals(List.of(
            new CustomerRiskFacts("reactive-partial", "LOW", 40),
            new CustomerRiskFacts("reactive-partial", "MEDIUM", 72),
            new CustomerRiskFacts("reactive-partial", "HIGH", 91)), retry);
    }

    private QueryOutcome<Object> invoke(Object input, JpaFindOneConfiguration configuration) throws Exception {
        return onVertxContext(() -> operation().query(new QueryInvocation<>(
            input, configuration, cast(CustomerRiskFacts.class), ConnectorExecutionContext.empty())));
    }

    @SuppressWarnings("unchecked")
    private QueryOperation<Object, JpaFindOneConfiguration, Object> operation() {
        return (QueryOperation<Object, JpaFindOneConfiguration, Object>) connector.operations().stream()
            .filter(QueryOperation.class::isInstance)
            .filter(candidate -> "find.one".equals(candidate.id()))
            .findFirst()
            .orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private StreamingQueryOperation<Object, JpaFindManyConfiguration, Object> streamingOperation() {
        return (StreamingQueryOperation<Object, JpaFindManyConfiguration, Object>) connector.operations().stream()
            .filter(StreamingQueryOperation.class::isInstance)
            .filter(candidate -> "find.many".equals(candidate.id()))
            .findFirst()
            .orElseThrow();
    }

    private void replaceWith(ReactiveCustomerRiskEntity... entities) throws Exception {
        onVertxContext(() -> sessionFactory.withTransaction(session ->
            session.createMutationQuery("delete from " + ReactiveCustomerRiskEntity.class.getName())
                .executeUpdate()
                .chain(() -> entities.length == 0
                    ? io.smallrye.mutiny.Uni.createFrom().voidItem()
                    : session.persistAll((Object[]) entities)))
            .subscribeAsCompletionStage());
    }

    private <T> T onVertxContext(Supplier<CompletionStage<T>> action) throws Exception {
        CompletableFuture<T> result = new CompletableFuture<>();
        Context context = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
        VertxContextSafetyToggle.setContextSafe(context, true);
        context.runOnContext(ignored -> {
            try {
                assertTrue(Vertx.currentContext().isEventLoopContext());
                action.get().whenComplete((value, failure) -> {
                    if (failure == null) {
                        result.complete(value);
                    } else {
                        result.completeExceptionally(failure);
                    }
                });
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
        });
        return result.get(30, TimeUnit.SECONDS);
    }

    private static <T> CompletionStage<T> withExecutionContext(
        PipelineExecutionContext context,
        Supplier<CompletionStage<T>> action
    ) {
        PipelineExecutionContextHolder.set(context);
        try {
            return action.get().whenComplete((ignored, failure) -> PipelineExecutionContextHolder.clear());
        } catch (Throwable failure) {
            PipelineExecutionContextHolder.clear();
            throw failure;
        }
    }

    private static JpaFindOneConfiguration baseConfiguration() {
        return configuration(Map.of("customerId", new JpaPredicate("eq", List.of("input.customerId"))));
    }

    private static JpaFindOneConfiguration configuration(Map<String, JpaPredicate> where) {
        return new JpaFindOneConfiguration(
            ReactiveCustomerRiskEntity.class.getName(), where,
            Optional.of(Map.of("customerId", "customerId", "riskBand", "riskBand", "score", "score")),
            Optional.empty(), Optional.empty(), Optional.of("single"));
    }

    private static JpaFindManyConfiguration manyConfiguration(Optional<Integer> limit) {
        java.util.LinkedHashMap<String, String> orderBy = new java.util.LinkedHashMap<>();
        orderBy.put("score", "asc");
        orderBy.put("id", "asc");
        return new JpaFindManyConfiguration(
            ReactiveCustomerRiskEntity.class.getName(),
            Map.of("customerId", new JpaPredicate("eq", List.of("input.customerId"))),
            Optional.of(Map.of("customerId", "customerId", "riskBand", "riskBand", "score", "score")),
            orderBy,
            List.of("id"),
            limit);
    }

    @SuppressWarnings("unchecked")
    private static Class<Object> cast(Class<?> type) {
        return (Class<Object>) type;
    }

    record CustomerRiskLookup(String customerId, int minimumScore) {
    }

    record CustomerRiskFacts(String customerId, String riskBand, int score) {
    }

    static final class ReactiveCustomerRiskMapper implements Mapper<CustomerRiskFacts, ReactiveCustomerRiskEntity> {
        private final AtomicBoolean failOnce;

        ReactiveCustomerRiskMapper() {
            this(false);
        }

        ReactiveCustomerRiskMapper(boolean failOnce) {
            this.failOnce = new AtomicBoolean(failOnce);
        }

        @Override
        public CustomerRiskFacts fromExternal(ReactiveCustomerRiskEntity external) {
            if (external.score == 72 && failOnce.compareAndSet(true, false)) {
                throw new IllegalStateException("partial reactive projection failure");
            }
            String riskBand = external.riskBand.startsWith("encoded:")
                ? external.riskBand.substring("encoded:".length())
                : external.riskBand;
            return new CustomerRiskFacts(external.customerId, riskBand, external.score);
        }

        @Override
        public ReactiveCustomerRiskEntity toExternal(CustomerRiskFacts domain) {
            return new ReactiveCustomerRiskEntity(
                domain.customerId(), "encoded:" + domain.riskBand(), domain.score(), "ACTIVE");
        }
    }

    public static final class PipelineConfiguration implements QuarkusTestResourceLifecycleManager {
        private String previous;
        private String previousValidation;

        @Override
        public Map<String, String> start() {
            previous = System.getProperty("pipeline.config");
            previousValidation = System.getProperty("smallrye.config.mapping.validate-unknown");
            System.setProperty("smallrye.config.mapping.validate-unknown", "false");
            System.setProperty("pipeline.config", Path.of(
                "src/test/resources/hibernate-reactive-query-pipeline.yaml").toAbsolutePath().toString());
            return Map.of();
        }

        @Override
        public void stop() {
            if (previous == null) {
                System.clearProperty("pipeline.config");
            } else {
                System.setProperty("pipeline.config", previous);
            }
            if (previousValidation == null) {
                System.clearProperty("smallrye.config.mapping.validate-unknown");
            } else {
                System.setProperty("smallrye.config.mapping.validate-unknown", previousValidation);
            }
        }
    }
}
