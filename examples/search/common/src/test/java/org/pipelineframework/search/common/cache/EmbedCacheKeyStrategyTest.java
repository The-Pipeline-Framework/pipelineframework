package org.pipelineframework.search.common.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;
import org.pipelineframework.search.common.dto.TokenBatchDto;

class EmbedCacheKeyStrategyTest {

  private static final PipelineContext TEST_CONTEXT = PipelineContext.fromHeaders("test-version", "record", "enabled");

  @Test
  void resolvesKeyForTokenBatchDto() {
    EmbedCacheKeyStrategy strategy = new EmbedCacheKeyStrategy();
    UUID docId = UUID.fromString("00000000-0000-0000-0000-000000000001");
    TokenBatchDto batch = TokenBatchDto.builder()
        .docId(docId)
        .batchIndex(2)
        .tokensHash("tokens-hash-1")
        .build();

    Optional<String> resolved = strategy.resolveKey(batch, TEST_CONTEXT);
    assertTrue(resolved.isPresent());
    assertEquals(EmbeddedChunk.class.getName()
        + ":doc=" + docId
        + ":batch=2:tokens=tokens-hash-1:vector=v1", resolved.get());
  }

  @Test
  void resolvesKeyForEmbeddedChunkDtoWithVersion() {
    EmbedCacheKeyStrategy strategy = new EmbedCacheKeyStrategy();
    UUID docId = UUID.fromString("00000000-0000-0000-0000-000000000002");
    EmbeddedChunkDto chunk = EmbeddedChunkDto.builder()
        .docId(docId)
        .batchIndex(3)
        .tokensHash("tokens-hash-2")
        .vectorVersion("vec-v2")
        .build();

    Optional<String> resolved = strategy.resolveKey(chunk, TEST_CONTEXT);
    assertTrue(resolved.isPresent());
    assertEquals(EmbeddedChunk.class.getName()
        + ":doc=" + docId
        + ":batch=3:tokens=tokens-hash-2:vector=vec-v2", resolved.get());
  }
}
