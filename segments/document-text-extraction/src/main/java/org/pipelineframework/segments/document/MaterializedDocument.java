package org.pipelineframework.segments.document;

import java.nio.file.Path;
import java.util.Objects;

/** File representation supplied through the framework's ordinary representation-provider semantics. */
public record MaterializedDocument(String sourceId, String fileName, String contentType, Path content) {
    public MaterializedDocument {
        sourceId = Objects.requireNonNull(sourceId, "source ID must not be null");
        fileName = Objects.requireNonNull(fileName, "document file name must not be null");
        contentType = Objects.requireNonNull(contentType, "document content type must not be null");
        content = Objects.requireNonNull(content, "materialized document path must not be null");
    }
}
