package org.pipelineframework.connector.embedding;

import java.util.List;

/** Portable result of one embedding observation. */
public record EmbeddingResult(String itemId, String text, List<Float> values) {
    public EmbeddingResult {
        itemId = EmbeddingValues.requireText(itemId, "embedding item ID");
        text = EmbeddingValues.requireText(text, "embedding text");
        values = EmbeddingValues.requireVector(values, "embedding values");
    }
}
