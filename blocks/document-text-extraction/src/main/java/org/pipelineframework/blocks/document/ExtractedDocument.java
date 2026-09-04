package org.pipelineframework.blocks.document;

import java.util.Objects;

/** Canonical result produced by the packaged extraction block. */
public record ExtractedDocument(String sourceId, String text, ExtractionDiagnostics diagnostics) {
    public ExtractedDocument {
        sourceId = Objects.requireNonNull(sourceId, "source ID must not be null");
        text = Objects.requireNonNull(text, "document text must not be null");
        diagnostics = Objects.requireNonNull(diagnostics, "extraction diagnostics must not be null");
    }
}
