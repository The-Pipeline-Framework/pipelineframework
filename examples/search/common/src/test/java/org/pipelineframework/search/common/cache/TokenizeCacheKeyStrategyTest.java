package org.pipelineframework.search.common.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;
import org.pipelineframework.search.common.dto.TokenBatchDto;

class TokenizeCacheKeyStrategyTest {

    @Test
    void resolvesKeyForParsedDocumentDto() {
        TokenizeCacheKeyStrategy strategy = new TokenizeCacheKeyStrategy();
        ParsedDocumentDto parsed = ParsedDocumentDto.builder()
            .contentHash("content-hash-1")
            .build();

        Optional<String> resolved = strategy.resolveKey(parsed, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());
        assertEquals(TokenBatch.class.getName() + ":content-hash-1:model=v1", resolved.get());
    }

    @Test
    void resolvesKeyForTokenBatchDto() {
        TokenizeCacheKeyStrategy strategy = new TokenizeCacheKeyStrategy();
        TokenBatchDto batch = TokenBatchDto.builder()
            .contentHash("content-hash-2")
            .build();

        Optional<String> resolved = strategy.resolveKey(batch, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());
        assertEquals(TokenBatch.class.getName() + ":content-hash-2:model=v1", resolved.get());
    }
}
