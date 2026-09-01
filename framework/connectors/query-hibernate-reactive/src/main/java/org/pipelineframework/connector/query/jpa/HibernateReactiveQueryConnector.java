package org.pipelineframework.connector.query.jpa;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.hibernate.reactive.mutiny.Mutiny;
import org.jboss.logging.Logger;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.QueryStream;
import org.pipelineframework.connector.StreamingQueryOperation;

/** Non-blocking Hibernate Reactive Query provider backed directly by {@link Mutiny.SessionFactory}. */
@ApplicationScoped
public final class HibernateReactiveQueryConnector implements ConnectorProvider<Void> {
    private static final Logger LOG = Logger.getLogger(HibernateReactiveQueryConnector.class);
    static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("hibernate.reactive.query");

    private final Optional<Instance<Mutiny.SessionFactory>> sessionFactory;
    private final HibernateReactiveFindOneOperation findOneOperation = new HibernateReactiveFindOneOperation();
    private final HibernateReactiveFindManyOperation findManyOperation = new HibernateReactiveFindManyOperation();

    /** Side-effect-free constructor used by connector artifact packaging. */
    public HibernateReactiveQueryConnector() {
        this.sessionFactory = Optional.empty();
    }

    @Inject
    public HibernateReactiveQueryConnector(Instance<Mutiny.SessionFactory> sessionFactory) {
        this.sessionFactory = Optional.of(sessionFactory);
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
        return List.of(findOneOperation, findManyOperation);
    }

    private CompletionStage<Object> queryOne(
        JpaQueryPlan plan,
        Object input,
        Class<Object> outputType,
        Optional<Function<Object, ?>> localResultMapper
    ) {
        if (sessionFactory.isEmpty() || !sessionFactory.orElseThrow().isResolvable()) {
            return CompletableFuture.failedStage(new IllegalStateException(
                "No Hibernate Reactive SessionFactory is available for connector hibernate.reactive.query"));
        }
        Mutiny.SessionFactory factory = sessionFactory.orElseThrow().get();
        Uni<Object> query = factory.withSession(session -> {
            Mutiny.SelectionQuery<?> selection = session
                .createSelectionQuery(plan.toHql(), plan.entityType())
                .setMaxResults(plan.maxResults());
            plan.bindings(input).forEach(selection::setParameter);
            return selection.getResultList().map(rows -> {
                Object external = HibernateQueryResult.projectSingle(plan, rows, outputType);
                Object result = localResultMapper.isPresent()
                    ? localResultMapper.orElseThrow().apply(external)
                    : external;
                if (result == null) {
                    throw new IllegalStateException("Hibernate Reactive query local result mapper returned null");
                }
                return result;
            });
        });
        return query.subscribeAsCompletionStage();
    }

    private final class HibernateReactiveFindOneOperation
        implements QueryOperation<Object, JpaFindOneConfiguration, Object> {
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
        @SuppressWarnings("unchecked")
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
                (Class<Object>) invocation.outputType(),
                (Optional<Function<Object, ?>>) (Optional<?>) invocation.localResultMapper())
                .handle((output, failure) -> outcome(id(), output, failure));
        }
    }

    private final class HibernateReactiveFindManyOperation
        implements StreamingQueryOperation<Object, JpaFindManyConfiguration, Object> {
        private static final int MAXIMUM_WINDOW = 64;
        private static final ConnectorConfigSchema<JpaFindManyConfiguration> CONFIGURATION_SCHEMA =
            ConnectorConfigSchema.record(JpaFindManyConfiguration.class, "jpa.query.find.many", 1);

        @Override
        public String id() {
            return "find.many";
        }

        @Override
        public Optional<ConnectorConfigSchema<JpaFindManyConfiguration>> configurationSchema() {
            return Optional.of(CONFIGURATION_SCHEMA);
        }

        @Override
        @SuppressWarnings("unchecked")
        public QueryStream<Object> query(QueryInvocation<Object, JpaFindManyConfiguration, Object> invocation) {
            JpaQueryPlan plan = JpaQueryPlan.fromMany(id(), invocation.configuration());
            if (sessionFactory.isEmpty() || !sessionFactory.orElseThrow().isResolvable()) {
                throw new IllegalStateException(
                    "No Hibernate Reactive SessionFactory is available for connector hibernate.reactive.query");
            }
            JpaQueryPlan.OrderingGuard ordering = plan.orderingGuard();
            Optional<Function<Object, ?>> localMapper =
                (Optional<Function<Object, ?>>) (Optional<?>) invocation.localResultMapper();
            HibernateReactiveWindowPublisher<Object> publisher = new HibernateReactiveWindowPublisher<>(
                sessionFactory.orElseThrow().get(),
                (session, offset, size) -> {
                    Mutiny.SelectionQuery<?> selection = session
                        .createSelectionQuery(plan.toHql(), plan.entityType())
                        .setFirstResult(offset)
                        .setMaxResults(size);
                    plan.bindings(invocation.input()).forEach(selection::setParameter);
                    return selection.getResultList().map(rows -> rows.stream().map(entity -> {
                        ordering.validateNext(entity);
                        Object external = JpaQueryProjection.project(
                            entity, (Class<Object>) invocation.outputType(), plan.projection());
                        Object result = localMapper.isPresent()
                            ? localMapper.orElseThrow().apply(external)
                            : external;
                        if (result == null) {
                            throw new IllegalStateException(
                                "Hibernate Reactive streaming query local result mapper returned null");
                        }
                        return result;
                    }).toList());
                },
                MAXIMUM_WINDOW,
                plan.streamingLimit());
            return new QueryStream<>(publisher, publisher.termination());
        }
    }

    private static QueryOutcome<Object> outcome(String queryId, Object output, Throwable failure) {
        if (failure == null) {
            return new QueryOutcome.Found<>(output);
        }
        Throwable cause = unwrap(failure);
        if (cause instanceof HibernateQueryResult.NotFoundException) {
            return new QueryOutcome.NotFound<>("not-found");
        }
        if (cause instanceof HibernateQueryResult.MultipleResultsException) {
            return new QueryOutcome.TerminalFailure<>("multiple-results");
        }
        LOG.errorf(cause, "Hibernate Reactive query %s failed unexpectedly", queryId);
        return new QueryOutcome.TerminalFailure<>("hibernate-reactive-query-failed");
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
