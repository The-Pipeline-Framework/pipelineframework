package org.pipelineframework.examples.rag.indexer.domain;

import java.util.Objects;

public record Chunk(String sourceId, String chunkId, String content, int index) {
    public Chunk {
        sourceId = Objects.requireNonNull(sourceId, "source ID must not be null");
        chunkId = Objects.requireNonNull(chunkId, "chunk ID must not be null");
        content = Objects.requireNonNull(content, "chunk content must not be null");
        if (index < 0) throw new IllegalArgumentException("chunk index must not be negative");
    }
}
