package org.pipelineframework.connector.query.jpa;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.TypedQuery;

import org.pipelineframework.query.FrameworkQueryConnector;
import org.pipelineframework.query.QueryRequest;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;

/**
 * First-party captured query connector for declarative JPA entity reads.
 */
@ApplicationScoped
public class JpaQueryConnector implements FrameworkQueryConnector, ConnectorProvider<Void> {
    static final String CONNECTOR_NAME = "jpa";
    static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("jpa.query");

    private final Optional<Instance<EntityManagerFactory>> entityManagerFactory;
    private final Optional<ExecutorService> blockingExecutor;

    /** Side-effect-free constructor used by connector artifact packaging. */
    public JpaQueryConnector() {
        this.entityManagerFactory = Optional.empty();
        this.blockingExecutor = Optional.empty();
    }

    @Inject
    public JpaQueryConnector(Instance<EntityManagerFactory> entityManagerFactory) {
        this.entityManagerFactory = Optional.of(entityManagerFactory);
        this.blockingExecutor = Optional.of(Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("tpf-jpa-query-", 0).factory()));
    }

    @PreDestroy
    void closeExecutor() {
        blockingExecutor.ifPresent(ExecutorService::close);
    }

    @Override
    public String connectorName() {
        return CONNECTOR_NAME;
    }

    @Override
    public ConnectorProviderId id() {
        return PROVIDER_ID;
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return List.of(new JpaFindOneOperation());
    }

    @Override
    public <O> CompletionStage<O> queryOne(QueryRequest<?> request, Class<O> outputType) {
        try {
            return queryOne(
                JpaQueryPlan.from(request.descriptor().queryId(),
                    JpaPipelineQueryAdapter.configuration(request.descriptor().jpa())),
                request.input(),
                outputType);
        } catch (RuntimeException ex) {
            return CompletableFuture.failedStage(ex);
        }
    }

    private <O> CompletionStage<O> queryOne(JpaQueryPlan plan, Object input, Class<O> outputType) {
        return queryOne(plan, input, outputType, Optional.empty()).thenApply(outputType::cast);
    }

    private <O> CompletionStage<Object> queryOne(
        JpaQueryPlan plan,
        Object input,
        Class<O> outputType,
        Optional<Function<O, ?>> localResultMapper
    ) {
        if (entityManagerFactory.isEmpty() || !entityManagerFactory.orElseThrow().isResolvable()) {
            return CompletableFuture.failedStage(new IllegalStateException(
                "No JPA EntityManagerFactory is available for connector jpa"));
        }
        try {
            return CompletableFuture.supplyAsync(
                () -> queryBlocking(plan, input, outputType, localResultMapper),
                blockingExecutor.orElseThrow());
        } catch (RuntimeException ex) {
            return CompletableFuture.failedStage(ex);
        }
    }

    private <O> Object queryBlocking(
        JpaQueryPlan plan,
        Object input,
        Class<O> outputType,
        Optional<Function<O, ?>> localResultMapper
    ) {
        EntityManager entityManager = entityManagerFactory.orElseThrow().get().createEntityManager();
        try {
            TypedQuery<?> query = entityManager.createQuery(plan.toHql(), plan.entityType())
                .setFlushMode(FlushModeType.COMMIT)
                .setMaxResults(plan.maxResults());
            plan.bindings(input).forEach(query::setParameter);
            O external = projectSingle(plan, query.getResultList(), outputType);
            Object result = localResultMapper.isPresent()
                ? localResultMapper.orElseThrow().apply(external)
                : external;
            if (result == null) {
                throw new IllegalStateException("JPA query local result mapper returned null");
            }
            return result;
        } finally {
            entityManager.close();
        }
    }

    private <O> O projectSingle(JpaQueryPlan plan, List<?> rows, Class<O> outputType) {
        if (rows.isEmpty()) {
            throw new JpaNotFoundException("JPA query '" + plan.queryId() + "' returned no rows");
        }
        if (!plan.firstResultOnly() && rows.size() > 1) {
            throw new JpaMultipleResultsException("JPA query '" + plan.queryId() + "' returned multiple rows");
        }
        return JpaQueryProjection.project(rows.getFirst(), outputType, plan.projection());
    }

    private final class JpaFindOneOperation implements QueryOperation<Object, JpaFindOneConfiguration, Object> {
        private static final ConnectorConfigSchema<JpaFindOneConfiguration> CONFIGURATION_SCHEMA =
            ConnectorConfigSchema.record(JpaFindOneConfiguration.class, "jpa.query.find.one", 1);

        @Override
        public String id() {
            return "find.one";
        }

        @Override
        public QueryCapabilities capabilities() {
            return QueryCapabilities.cacheable();
        }

        @Override
        public Optional<ConnectorConfigSchema<JpaFindOneConfiguration>> configurationSchema() {
            return Optional.of(CONFIGURATION_SCHEMA);
        }

        @Override
        public CompletionStage<QueryOutcome<Object>> query(
            QueryInvocation<Object, JpaFindOneConfiguration, Object> invocation
        ) {
            JpaQueryPlan plan;
            try {
                plan = JpaQueryPlan.from(id(), invocation.configuration());
            } catch (RuntimeException failure) {
                return CompletableFuture.failedStage(failure);
            }
            return queryOne(
                plan,
                invocation.input(),
                invocation.outputType(),
                invocation.localResultMapper()).handle((output, failure) -> {
                if (failure == null) {
                    return new QueryOutcome.Found<>(output);
                }
                Throwable cause = unwrap(failure);
                if (cause instanceof JpaNotFoundException) {
                    return new QueryOutcome.NotFound<>("not-found");
                }
                if (cause instanceof JpaMultipleResultsException) {
                    return new QueryOutcome.TerminalFailure<>("multiple-results");
                }
                return new QueryOutcome.TerminalFailure<>("jpa-query-failed");
            });
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class JpaNotFoundException extends IllegalStateException {
        private JpaNotFoundException(String message) {
            super(message);
        }
    }

    private static final class JpaMultipleResultsException extends IllegalStateException {
        private JpaMultipleResultsException(String message) {
            super(message);
        }
    }
}
