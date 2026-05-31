package org.pipelineframework.search.common.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;
import org.pipelineframework.search.common.dto.TokenBatchDto;

class TokenizeCacheKeyStrategyTest {

    private static final PipelineContext TEST_CONTEXT = PipelineContext.fromHeaders("test-version", "record", "enabled");

    @Test
    void resolvesKeyForParsedDocumentDto() {
        TokenizeCacheKeyStrategy strategy = new TokenizeCacheKeyStrategy();
        ParsedDocumentDto parsed = ParsedDocumentDto.builder()
            .contentHash("content-hash-1")
            .build();

        Optional<String> resolved = strategy.resolveKey(parsed, TEST_CONTEXT);
        assertTrue(resolved.isPresent());
        assertEquals(TokenBatch.class.getName() + ":content-hash-1:model=v1", resolved.get());
    }

    @Test
    void resolvesKeyForTokenBatchDto() {
        TokenizeCacheKeyStrategy strategy = new TokenizeCacheKeyStrategy();
        TokenBatchDto batch = TokenBatchDto.builder()
            .contentHash("content-hash-2")
            .batchIndex(3)
            .build();

        Optional<String> resolved = strategy.resolveKey(batch, TEST_CONTEXT);
        assertTrue(resolved.isPresent());
        assertEquals(TokenBatch.class.getName() + ":content-hash-2:model=v1:batch=3", resolved.get());
    }

    @Test
    void rejectsNegativeTokenBatchIndex() {
        TokenizeCacheKeyStrategy strategy = new TokenizeCacheKeyStrategy();
        TokenBatchDto batch = TokenBatchDto.builder()
            .contentHash("content-hash-3")
            .batchIndex(-1)
            .build();

        IllegalArgumentException error =
            assertThrows(IllegalArgumentException.class, () -> strategy.resolveKey(batch, TEST_CONTEXT));
        assertTrue(error.getMessage().contains("batchIndex must be >= 0"));
        assertTrue(error.getMessage().contains("-1"));
    }
}
