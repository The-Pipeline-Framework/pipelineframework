package org.pipelineframework.connector.embedding;

import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperation;

/** Provider-neutral unary embedding Query operation. */
public interface EmbeddingQueryOperation
    extends QueryOperation<EmbeddingRequest, ConnectorConfigurationDocument, EmbeddingResult> {
    @Override
    default String id() {
        return "embed";
    }

    @Override
    default QueryCapabilities capabilities() {
        return QueryCapabilities.cacheable();
    }
}
