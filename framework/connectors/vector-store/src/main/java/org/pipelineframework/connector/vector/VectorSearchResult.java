package org.pipelineframework.connector.vector;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Successful vector search result, including the ordinary empty-match case. */
public record VectorSearchResult(String queryId, String queryText, List<VectorMatch> matches) {
    private static final Comparator<VectorMatch> MATCH_ORDER =
        Comparator.comparing(VectorMatch::score).reversed().thenComparing(VectorMatch::itemId);

    public VectorSearchResult {
        queryId = VectorValues.requireText(queryId, "vector query ID");
        queryText = VectorValues.requireText(queryText, "vector query text");
        matches = List.copyOf(Objects.requireNonNull(matches, "vector matches must not be null"));
        for (int index = 1; index < matches.size(); index++) {
            if (MATCH_ORDER.compare(matches.get(index - 1), matches.get(index)) > 0) {
                throw new IllegalArgumentException(
                    "vector matches must be ordered by descending score and ascending item ID for ties");
            }
        }
    }
}
