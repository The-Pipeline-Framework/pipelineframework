package org.pipelineframework.search.common.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.dto.IndexAckDto;
import org.pipelineframework.search.common.dto.TokenBatchDto;

class IndexCacheKeyStrategyTest {

    @Test
    void resolvesKeyForTokenBatchDto() {
        IndexCacheKeyStrategy strategy = new IndexCacheKeyStrategy();
        TokenBatchDto batch = TokenBatchDto.builder()
            .tokensHash("tokens-hash-1")
            .build();

        Optional<String> resolved = strategy.resolveKey(batch, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());
        assertEquals(IndexAck.class.getName() + ":tokens-hash-1:schema=v1", resolved.get());
    }

    @Test
    void resolvesKeyForIndexAckDtoWithVersion() {
        IndexCacheKeyStrategy strategy = new IndexCacheKeyStrategy();
        IndexAckDto ack = IndexAckDto.builder()
            .tokensHash("tokens-hash-2")
            .indexVersion("v2")
            .build();

        Optional<String> resolved = strategy.resolveKey(ack, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());
        assertEquals(IndexAck.class.getName() + ":tokens-hash-2:schema=v2", resolved.get());
    }
}
