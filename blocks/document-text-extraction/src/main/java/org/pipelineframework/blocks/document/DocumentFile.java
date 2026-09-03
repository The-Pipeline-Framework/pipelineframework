package org.pipelineframework.blocks.document;

import java.util.Objects;
import org.pipelineframework.repository.PayloadReference;

/** Canonical document reference accepted by the packaged extraction block. */
public record DocumentFile(String sourceId, String fileName, String contentType, PayloadReference content) {
    public DocumentFile {
        sourceId = requireText(sourceId, "source ID");
        fileName = requireText(fileName, "document file name");
        contentType = requireText(contentType, "document content type");
        content = Objects.requireNonNull(content, "document content reference must not be null");
    }

    private static String requireText(String value, String label) {
        Objects.requireNonNull(value, label + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
        return value.strip();
    }
}
