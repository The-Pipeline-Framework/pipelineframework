package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;

/**
 * Query-family operation contract. Query outcome semantics are defined by the query runtime work.
 */
public interface QueryOperation<I, C, O> extends ConnectorOperation {
    CompletionStage<QueryOutcome<O>> query(QueryInvocation<I, C> invocation);
}
