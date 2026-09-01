package org.pipelineframework.connector.vector;

import java.util.List;

/** Portable request to index one already-vectorized item. */
public record VectorUpsertRequest(String itemId, String content, List<Float> values) {
    public VectorUpsertRequest {
        itemId = VectorValues.requireText(itemId, "vector item ID");
        content = VectorValues.requireText(content, "vector content");
        values = VectorValues.requireVector(values, "vector values");
    }
}
