package org.pipelineframework.examples.rag.indexer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.vector.VectorUpsertResult;
import org.pipelineframework.examples.rag.document.DocumentExtractionLimitException;
import org.pipelineframework.examples.rag.document.DocumentTextExtractor;
import org.pipelineframework.examples.rag.indexer.domain.MaterializedDocument;
import org.pipelineframework.examples.rag.indexer.domain.ParsedDocument;
import org.pipelineframework.examples.rag.support.ChunkId;
import org.pipelineframework.objectingest.ObjectSnapshot;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.step.NonRetryableException;

class IndexerServicesTest {
    @TempDir java.nio.file.Path directory;

    @Test void parsesUtf8AndCreatesStableFanOut() throws Exception {
        var file = directory.resolve("manual.txt");
        Files.writeString(file, "one two three four");
        var parsed = ExtractDocumentTextService.extract(new MaterializedDocument(
            "source#v2", "manual.txt", "text/plain", file));
        var chunks = ChunkDocumentService.chunks(parsed);
        assertEquals(1, chunks.size());
        assertEquals("source#v2", ChunkId.decode(chunks.getFirst().chunkId()).sourceId());
        assertEquals(chunks, ChunkDocumentService.chunks(parsed));
        assertEquals("PLAIN_TEXT", parsed.diagnostics().format());
        assertEquals("CONTENT_TYPE", parsed.diagnostics().selectedBy());
        assertEquals(parsed.text().length(), parsed.diagnostics().extractedCharacters());
    }

    @Test void receiptPreservesFullSourceIdentity() {
        String first = ChunkId.encode("source#v2", 0, "a");
        String second = ChunkId.encode("source#v2", 1, "b");
        assertEquals("source#v2", IndexReceiptService.receipt(List.of(
            new VectorUpsertResult(first), new VectorUpsertResult(second))).sourceId());
        assertThrows(IllegalArgumentException.class, () -> IndexReceiptService.receipt(List.of()));
    }

    @Test void objectAdmissionCarriesOriginalNameAndContentTypeToExtraction() {
        var reference = new PayloadReference("filesystem", "/documents", "guide.pdf", "application/pdf", "raw",
            "checksum", 12, "v1", Map.of(), Optional.empty());
        var snapshot = new ObjectSnapshot("document-inbox", "filesystem", "/documents", "guide.pdf", "v1",
            "etag", 12, 1, "", Map.of(), reference, "", "/documents/guide.pdf");

        var document = new DocumentObjectMapper().map(snapshot);

        assertEquals("guide.pdf", document.fileName());
        assertEquals("application/pdf", document.contentType());
        assertEquals(reference, document.content());
    }

    @Test void convertsExtractionLimitFailuresToNonRetryableFailures() throws Exception {
        var file = directory.resolve("expanded.txt");
        Files.writeString(file, "123456");
        var document = new MaterializedDocument("source", "expanded.txt", "text/plain", file);

        var failure = assertThrows(NonRetryableException.class,
            () -> ExtractDocumentTextService.extract(document, new DocumentTextExtractor(10, 5)));

        assertInstanceOf(DocumentExtractionLimitException.class, failure.getCause());
    }
}
