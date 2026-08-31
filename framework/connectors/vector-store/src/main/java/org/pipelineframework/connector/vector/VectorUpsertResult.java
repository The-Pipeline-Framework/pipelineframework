package org.pipelineframework.connector.vector;

/** Portable acknowledgement of one vector upsert. */
public record VectorUpsertResult(String itemId) {
    public VectorUpsertResult {
        itemId = VectorValues.requireText(itemId, "vector item ID");
    }
}
