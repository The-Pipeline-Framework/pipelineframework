package org.pipelineframework.connector.query.jpa;

import java.util.List;

/** Shared unary cardinality and projection semantics for Hibernate Query providers. */
final class HibernateQueryResult {
    private HibernateQueryResult() {
    }

    static <O> O projectSingle(JpaQueryPlan plan, List<?> rows, Class<O> outputType) {
        if (rows.isEmpty()) {
            throw new NotFoundException("Hibernate query '" + plan.queryId() + "' returned no rows");
        }
        if (!plan.firstResultOnly() && rows.size() > 1) {
            throw new MultipleResultsException("Hibernate query '" + plan.queryId() + "' returned multiple rows");
        }
        return JpaQueryProjection.project(rows.getFirst(), outputType, plan.projection());
    }

    static final class NotFoundException extends IllegalStateException {
        private NotFoundException(String message) {
            super(message);
        }
    }

    static final class MultipleResultsException extends IllegalStateException {
        private MultipleResultsException(String message) {
            super(message);
        }
    }
}
