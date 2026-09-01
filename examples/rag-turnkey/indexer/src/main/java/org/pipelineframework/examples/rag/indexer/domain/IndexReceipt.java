package org.pipelineframework.examples.rag.indexer.domain;

import java.util.Objects;

public record IndexReceipt(String sourceId, int chunks) {
    public IndexReceipt {
        sourceId = Objects.requireNonNull(sourceId, "source ID must not be null");
        if (chunks < 1) throw new IllegalArgumentException("index receipt must contain at least one chunk");
    }
}
