package org.pipelineframework.examples.rag.indexer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.vector.VectorUpsertResult;
import org.pipelineframework.examples.rag.support.ChunkId;
import org.pipelineframework.objectingest.ObjectSnapshot;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.segments.document.ExtractedDocument;
import org.pipelineframework.segments.document.ExtractionDiagnostics;

class IndexerServicesTest {
    @Test void createsStableFanOutFromPackagedExtractionOutput() {
        var extracted = new ExtractedDocument("source#v2", "one two three four",
            new ExtractionDiagnostics("PLAIN_TEXT", "CONTENT_TYPE", "text/plain", 18, 18, List.of()));
        var chunks = ChunkDocumentService.chunks(extracted);
        assertEquals(1, chunks.size());
        assertEquals("source#v2", ChunkId.decode(chunks.getFirst().chunkId()).sourceId());
        assertEquals(chunks, ChunkDocumentService.chunks(extracted));
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
}
