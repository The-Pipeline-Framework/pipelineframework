package org.pipelineframework.connector.vector;

import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperation;

/** Provider-neutral Query operation for bounded similarity search. */
public interface VectorSearchQueryOperation
    extends QueryOperation<VectorSearchRequest, ConnectorConfigurationDocument, VectorSearchResult> {
    @Override
    default String id() {
        return "search";
    }

    @Override
    default QueryCapabilities capabilities() {
        return QueryCapabilities.cacheable();
    }
}
