package org.pipelineframework.examples.ragproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.vector.VectorUpsertResult;

class IndexReceiptServiceTest {
    private final IndexReceiptService service = new IndexReceiptService();

    @Test
    void removesOnlyTheGeneratedChunkSuffix() {
        var receipt = service.process(Multi.createFrom().items(
            new VectorUpsertResult("document#revision#0000"),
            new VectorUpsertResult("document#revision#0001"))).await().indefinitely();

        assertEquals("document#revision", receipt.documentId());
        assertEquals(2, receipt.chunks());
    }

    @Test
    void rejectsChunksFromDifferentFullDocumentIds() {
        assertThrows(IllegalArgumentException.class, () -> service.process(Multi.createFrom().items(
            new VectorUpsertResult("document#one#0000"),
            new VectorUpsertResult("document#two#0001"))).await().indefinitely());
    }
}
