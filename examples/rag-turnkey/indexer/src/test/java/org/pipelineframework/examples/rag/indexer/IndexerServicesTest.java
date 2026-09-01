package org.pipelineframework.examples.rag.indexer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.vector.VectorUpsertResult;
import org.pipelineframework.examples.rag.indexer.domain.MaterializedDocument;
import org.pipelineframework.examples.rag.indexer.domain.ParsedDocument;
import org.pipelineframework.examples.rag.support.ChunkId;

class IndexerServicesTest {
    @TempDir java.nio.file.Path directory;

    @Test void parsesUtf8AndCreatesStableFanOut() throws Exception {
        var file = directory.resolve("manual.txt");
        Files.writeString(file, "one two three four");
        var parsed = ParseDocumentService.parse(new MaterializedDocument("source#v2", file));
        var chunks = ChunkDocumentService.chunks(parsed);
        assertEquals(1, chunks.size());
        assertEquals("source#v2", ChunkId.decode(chunks.getFirst().chunkId()).sourceId());
        assertEquals(chunks, ChunkDocumentService.chunks(parsed));
    }

    @Test void receiptPreservesFullSourceIdentity() {
        String first = ChunkId.encode("source#v2", 0, "a");
        String second = ChunkId.encode("source#v2", 1, "b");
        assertEquals("source#v2", IndexReceiptService.receipt(List.of(
            new VectorUpsertResult(first), new VectorUpsertResult(second))).sourceId());
        assertThrows(IllegalArgumentException.class, () -> IndexReceiptService.receipt(List.of()));
    }
}
