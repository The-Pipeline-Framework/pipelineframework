package org.pipelineframework.blocks.document;

import java.nio.file.Path;
import java.util.Objects;

public record DocumentExtractionRequest(Path content, String fileName, String contentType) {
    public DocumentExtractionRequest {
        content = Objects.requireNonNull(content, "document path must not be null");
        fileName = requireText(fileName, "document file name");
        contentType = requireText(contentType, "document content type");
    }

    private static String requireText(String value, String label) {
        Objects.requireNonNull(value, label + " must not be null");
        if (value.isBlank()) throw new IllegalArgumentException(label + " must not be blank");
        return value.strip();
    }
}
