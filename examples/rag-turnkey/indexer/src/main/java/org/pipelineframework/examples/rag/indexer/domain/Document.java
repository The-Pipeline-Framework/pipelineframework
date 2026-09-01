package org.pipelineframework.examples.rag.indexer.domain;

import java.util.Objects;
import org.pipelineframework.repository.PayloadReference;

public record Document(String sourceId, String fileName, String contentType, PayloadReference content) {
    public Document {
        sourceId = Objects.requireNonNull(sourceId, "source ID must not be null");
        fileName = Objects.requireNonNull(fileName, "document file name must not be null");
        contentType = Objects.requireNonNull(contentType, "document content type must not be null");
        content = Objects.requireNonNull(content, "document content reference must not be null");
    }
}
