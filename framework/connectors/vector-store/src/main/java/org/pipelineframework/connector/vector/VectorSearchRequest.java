package org.pipelineframework.connector.vector;

import java.util.List;

/** Portable request for bounded similarity search. */
public record VectorSearchRequest(String queryId, String queryText, List<Float> values, Integer limit) {
    public VectorSearchRequest {
        queryId = VectorValues.requireText(queryId, "vector query ID");
        queryText = VectorValues.requireText(queryText, "vector query text");
        values = VectorValues.requireVector(values, "vector query values");
        if (limit == null || limit <= 0) {
            throw new IllegalArgumentException("vector search limit must be positive");
        }
    }
}
