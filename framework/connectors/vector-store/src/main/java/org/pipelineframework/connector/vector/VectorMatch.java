package org.pipelineframework.connector.vector;

/** One ordered similarity match; scores are response-local ordering values. */
public record VectorMatch(String itemId, String content, Float score) {
    public VectorMatch {
        itemId = VectorValues.requireText(itemId, "vector match item ID");
        content = VectorValues.requireText(content, "vector match content");
        if (score == null || !Float.isFinite(score)) {
            throw new IllegalArgumentException("vector match score must be finite");
        }
    }
}
