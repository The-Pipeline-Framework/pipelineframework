package org.pipelineframework.connector.query.jpa;

import java.util.List;
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

/**
 * First-party captured query connector for declarative JPA entity reads.
 */
@ApplicationScoped
public class JpaQueryConnector implements FrameworkQueryConnector {
    static final String CONNECTOR_NAME = "jpa";

    private final Instance<Mutiny.SessionFactory> sessionFactory;

    @Inject
    public JpaQueryConnector(Instance<Mutiny.SessionFactory> sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public String connectorName() {
        return CONNECTOR_NAME;
    }

    @Override
    public <O> CompletionStage<O> queryOne(QueryRequest<?> request, Class<O> outputType) {
        if (sessionFactory == null || sessionFactory.isUnsatisfied()) {
            return CompletableFuture.failedStage(new IllegalStateException(
                "No Hibernate Reactive SessionFactory is available for connector jpa"));
        }
        try {
            JpaQueryPlan plan = JpaQueryPlan.from(request.descriptor());
            Class<?> entityType = plan.entityType();
            return sessionFactory.get().withSession(session -> executeQuery(session, plan, request.input(), entityType))
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
            .setMaxResults(2);
        plan.bindings(input).forEach(query::setParameter);
        return query.getResultList().onItem().transform(rows -> (List<?>) rows);
    }

    private <O> O projectSingle(JpaQueryPlan plan, List<?> rows, Class<O> outputType) {
        if (rows.isEmpty()) {
            throw new IllegalStateException("JPA query '" + plan.queryId() + "' returned no rows");
        }
        if (rows.size() > 1) {
            throw new IllegalStateException("JPA query '" + plan.queryId() + "' returned multiple rows");
        }
        return JpaQueryProjection.project(rows.getFirst(), outputType, plan.projection());
    }
}
