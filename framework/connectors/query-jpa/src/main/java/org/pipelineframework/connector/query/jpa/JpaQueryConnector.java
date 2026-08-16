package org.pipelineframework.connector.query.jpa;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.hibernate.FlushMode;
import org.hibernate.reactive.mutiny.Mutiny;
import org.pipelineframework.query.FrameworkQueryConnector;
import org.pipelineframework.query.QueryRequest;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;

/**
 * First-party captured query connector for declarative JPA entity reads.
 */
@ApplicationScoped
public class JpaQueryConnector implements FrameworkQueryConnector, ConnectorProvider<Void> {
    static final String CONNECTOR_NAME = "jpa";
    static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("jpa.query");

    private final Optional<Instance<Mutiny.SessionFactory>> sessionFactory;

    /** Side-effect-free constructor used by connector artifact packaging. */
    public JpaQueryConnector() {
        this.sessionFactory = Optional.empty();
    }

    @Inject
    public JpaQueryConnector(Instance<Mutiny.SessionFactory> sessionFactory) {
        this.sessionFactory = Optional.of(sessionFactory);
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
            return queryOne(JpaQueryPlan.from(request.descriptor()), request.input(), outputType);
        } catch (RuntimeException ex) {
            return CompletableFuture.failedStage(ex);
        }
    }

    private <O> CompletionStage<O> queryOne(JpaQueryPlan plan, Object input, Class<O> outputType) {
        if (sessionFactory.isEmpty() || sessionFactory.orElseThrow().isUnsatisfied()) {
            return CompletableFuture.failedStage(new IllegalStateException(
                "No Hibernate Reactive SessionFactory is available for connector jpa"));
        }
        try {
            Class<?> entityType = plan.entityType();
            return sessionFactory.orElseThrow().get().withSession(session -> executeQuery(session, plan, input, entityType))
                .onItem().transform(rows -> projectSingle(plan, rows, outputType))
                .subscribeAsCompletionStage();
        } catch (RuntimeException ex) {
            return CompletableFuture.failedStage(ex);
        }
    }

    private Uni<List<?>> executeQuery(Mutiny.Session session, JpaQueryPlan plan, Object input, Class<?> entityType) {
        session.setDefaultReadOnly(true);
        Mutiny.SelectionQuery<?> query = session.createQuery(plan.toHql(), entityType)
            .setReadOnly(true)
            .setFlushMode(FlushMode.MANUAL)
            .setMaxResults(plan.maxResults());
        plan.bindings(input).forEach(query::setParameter);
        return query.getResultList().onItem().transform(rows -> (List<?>) rows);
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
            return queryOne(plan, invocation.input(), invocation.outputType()).handle((output, failure) -> {
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
