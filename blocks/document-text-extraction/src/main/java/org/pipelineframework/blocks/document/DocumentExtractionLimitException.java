package org.pipelineframework.blocks.document;

/** Deterministic rejection when a document exceeds an extraction resource limit. */
public final class DocumentExtractionLimitException extends IllegalArgumentException {
    public DocumentExtractionLimitException(String message) {
        super(message);
    }
}
