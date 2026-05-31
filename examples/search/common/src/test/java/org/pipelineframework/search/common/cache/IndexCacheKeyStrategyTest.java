package org.pipelineframework.search.common.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;
import org.pipelineframework.search.common.dto.IndexAckDto;

class IndexCacheKeyStrategyTest {

    @Test
    void resolvesKeyForEmbeddedChunkDto() {
        IndexCacheKeyStrategy strategy = new IndexCacheKeyStrategy();
        UUID docId = UUID.fromString("00000000-0000-0000-0000-000000000003");
        EmbeddedChunkDto chunk = EmbeddedChunkDto.builder()
            .docId(docId)
            .batchIndex(4)
            .tokensHash("tokens-hash-1")
            .build();

        Optional<String> resolved = strategy.resolveKey(chunk, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());
        assertEquals(IndexAck.class.getName()
            + ":doc=" + docId
            + ":batch=4:tokens=tokens-hash-1:schema=v1", resolved.get());
    }

    @Test
    void resolvesKeyForIndexAckDtoWithVersion() {
        IndexCacheKeyStrategy strategy = new IndexCacheKeyStrategy();
        UUID docId = UUID.fromString("00000000-0000-0000-0000-000000000004");
        IndexAckDto ack = IndexAckDto.builder()
            .docId(docId)
            .tokensHash("tokens-hash-2")
            .indexVersion("v2")
            .build();

        Optional<String> resolved = strategy.resolveKey(ack, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());
        assertEquals(IndexAck.class.getName()
            + ":doc=" + docId
            + ":tokens=tokens-hash-2:schema=v2", resolved.get());
    }
}
