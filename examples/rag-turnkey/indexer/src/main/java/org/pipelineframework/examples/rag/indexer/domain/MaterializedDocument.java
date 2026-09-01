package org.pipelineframework.examples.rag.indexer.domain;

import java.nio.file.Path;
import java.util.Objects;

public record MaterializedDocument(String sourceId, Path content) {
    public MaterializedDocument {
        sourceId = Objects.requireNonNull(sourceId, "source ID must not be null");
        content = Objects.requireNonNull(content, "materialized document path must not be null");
    }
}
