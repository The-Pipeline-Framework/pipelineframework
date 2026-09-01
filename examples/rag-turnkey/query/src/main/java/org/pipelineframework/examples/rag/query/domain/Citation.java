package org.pipelineframework.examples.rag.query.domain;

import java.util.Objects;

public record Citation(String sourceId, String chunkId, String excerpt) {
    public Citation {
        sourceId = Objects.requireNonNull(sourceId, "citation source ID must not be null");
        chunkId = Objects.requireNonNull(chunkId, "citation chunk ID must not be null");
        excerpt = Objects.requireNonNull(excerpt, "citation excerpt must not be null");
    }
}
