package org.pipelineframework.segments.document;

import java.util.List;
import java.util.Objects;

public record ExtractionDiagnostics(
    String format,
    String selectedBy,
    String contentType,
    long inputBytes,
    int extractedCharacters,
    List<String> notes
) {
    public ExtractionDiagnostics {
        format = Objects.requireNonNull(format, "document format must not be null");
        selectedBy = Objects.requireNonNull(selectedBy, "format selection must not be null");
        contentType = Objects.requireNonNull(contentType, "content type must not be null");
        if (inputBytes < 0) throw new IllegalArgumentException("input bytes must not be negative");
        if (extractedCharacters < 0) throw new IllegalArgumentException("extracted characters must not be negative");
        notes = List.copyOf(Objects.requireNonNull(notes, "extraction notes must not be null"));
    }
}
