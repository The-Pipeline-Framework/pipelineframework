package org.pipelineframework.segments.document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.step.NonRetryableException;

class ExtractDocumentTextServiceTest {
    @TempDir java.nio.file.Path directory;

    @Test void preservesSourceIdentityAndExtractionDiagnostics() throws Exception {
        var file = directory.resolve("manual.txt");
        Files.writeString(file, "one two three four");

        var extracted = ExtractDocumentTextService.extract(new MaterializedDocument(
            "source#v2", "manual.txt", "text/plain", file));

        assertEquals("source#v2", extracted.sourceId());
        assertEquals("one two three four", extracted.text());
        assertEquals("PLAIN_TEXT", extracted.diagnostics().format());
        assertEquals("CONTENT_TYPE", extracted.diagnostics().selectedBy());
    }

    @Test void convertsDeterministicLimitFailuresToNonRetryableFailures() throws Exception {
        var file = directory.resolve("expanded.txt");
        Files.writeString(file, "123456");
        var document = new MaterializedDocument("source", "expanded.txt", "text/plain", file);

        var failure = assertThrows(NonRetryableException.class,
            () -> ExtractDocumentTextService.extract(document, new DocumentTextExtractor(10, 5)));

        assertInstanceOf(DocumentExtractionLimitException.class, failure.getCause());
    }

    @Test void convertsInvalidDocumentFailuresToNonRetryableFailures() throws Exception {
        var file = directory.resolve("unsupported.bin");
        Files.writeString(file, "content");
        var document = new MaterializedDocument("source", "unsupported.bin", "application/octet-stream", file);

        var failure = assertThrows(NonRetryableException.class,
            () -> ExtractDocumentTextService.extract(document));

        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertEquals("unsupported document content type and extension: application/octet-stream, unsupported.bin",
            failure.getMessage());
    }
}
