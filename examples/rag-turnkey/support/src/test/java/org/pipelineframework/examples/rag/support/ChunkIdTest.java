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

    @Test void acceptsChunkIndexBoundaries() {
        assertEquals(0, ChunkId.decode(ChunkId.encode("source", 0, "first")).index());
        assertEquals(999_999, ChunkId.decode(ChunkId.encode("source", 999_999, "last")).index());
    }

    @Test void rejectsChunkIndexesOutsideTheEncodedRange() {
        assertThrows(IllegalArgumentException.class, () -> ChunkId.encode("source", -1, "before"));
        assertThrows(IllegalArgumentException.class, () -> ChunkId.encode("source", 1_000_000, "after"));
    }

    @Test void rejectsSourceIdsWithSurroundingWhitespace() {
        assertThrows(IllegalArgumentException.class, () -> ChunkId.encode(" source ", 0, "content"));
    }
}
