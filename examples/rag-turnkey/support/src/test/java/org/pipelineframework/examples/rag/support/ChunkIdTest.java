package org.pipelineframework.examples.rag.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ChunkIdTest {
    @Test void roundTripsSourcesContainingSeparators() {
        String encoded = ChunkId.encode("manuals/product#v2.txt", 7, "stable content");
        ChunkId decoded = ChunkId.decode(encoded);
        assertEquals("manuals/product#v2.txt", decoded.sourceId());
        assertEquals(7, decoded.index());
        assertEquals(encoded, ChunkId.encode(decoded.sourceId(), decoded.index(), "stable content"));
    }

    @Test void rejectsNonCanonicalIds() {
        assertThrows(IllegalArgumentException.class, () -> ChunkId.decode("plain#0001"));
    }
}
