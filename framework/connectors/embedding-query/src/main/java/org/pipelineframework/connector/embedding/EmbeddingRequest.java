package org.pipelineframework.connector.embedding;

/** Portable input for one embedding observation. */
public record EmbeddingRequest(String itemId, String text) {
    public EmbeddingRequest {
        itemId = EmbeddingValues.requireText(itemId, "embedding item ID");
        text = EmbeddingValues.requireText(text, "embedding text");
    }
}
