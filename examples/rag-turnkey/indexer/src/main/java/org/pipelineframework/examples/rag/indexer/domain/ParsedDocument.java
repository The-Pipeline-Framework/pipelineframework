package org.pipelineframework.examples.rag.indexer.domain;

import java.util.Objects;

public record ParsedDocument(String sourceId, String text, ExtractionDiagnostics diagnostics) {
    public ParsedDocument {
        sourceId = Objects.requireNonNull(sourceId, "source ID must not be null");
        text = Objects.requireNonNull(text, "document text must not be null");
        diagnostics = Objects.requireNonNull(diagnostics, "extraction diagnostics must not be null");
    }
}
