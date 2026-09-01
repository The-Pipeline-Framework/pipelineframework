package org.pipelineframework.examples.rag.query.domain;

import java.util.Objects;

public record RetrievedChunk(String sourceId, String chunkId, String excerpt, float score) {
    public RetrievedChunk {
        sourceId = Objects.requireNonNull(sourceId, "source ID must not be null");
        chunkId = Objects.requireNonNull(chunkId, "chunk ID must not be null");
        excerpt = Objects.requireNonNull(excerpt, "excerpt must not be null");
        if (!Float.isFinite(score)) throw new IllegalArgumentException("retrieval score must be finite");
    }
}
