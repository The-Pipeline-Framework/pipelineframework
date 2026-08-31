package org.pipelineframework.connector.vector;

import java.util.List;
import java.util.Objects;

/** Successful vector search result, including the ordinary empty-match case. */
public record VectorSearchResult(String queryId, String queryText, List<VectorMatch> matches) {
    public VectorSearchResult {
        queryId = VectorValues.requireText(queryId, "vector query ID");
        queryText = VectorValues.requireText(queryText, "vector query text");
        matches = List.copyOf(Objects.requireNonNull(matches, "vector matches must not be null"));
    }
}
