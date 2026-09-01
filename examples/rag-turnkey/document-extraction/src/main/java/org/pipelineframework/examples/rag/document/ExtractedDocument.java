package org.pipelineframework.examples.rag.document;

import java.util.Objects;

public record ExtractedDocument(String text, DocumentExtractionDiagnostics diagnostics) {
    public ExtractedDocument {
        text = Objects.requireNonNull(text, "extracted text must not be null");
        diagnostics = Objects.requireNonNull(diagnostics, "extraction diagnostics must not be null");
    }
}
