package org.pipelineframework.segments.document;

import java.util.Objects;

public record ExtractedText(String text, DocumentExtractionDiagnostics diagnostics) {
    public ExtractedText {
        text = Objects.requireNonNull(text, "extracted text must not be null");
        diagnostics = Objects.requireNonNull(diagnostics, "extraction diagnostics must not be null");
    }
}
