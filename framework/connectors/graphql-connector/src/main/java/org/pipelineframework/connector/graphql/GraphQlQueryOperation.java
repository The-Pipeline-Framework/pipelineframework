package org.pipelineframework.connector.graphql;

import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperation;

/** Provider-neutral live-only GraphQL Query operation. */
public interface GraphQlQueryOperation
    extends QueryOperation<GraphQlQueryRequest, ConnectorConfigurationDocument, GraphQlResponse> {
    @Override
    default String id() {
        return "execute.query";
    }

    @Override
    default QueryCapabilities capabilities() {
        return QueryCapabilities.conservative();
    }
}
