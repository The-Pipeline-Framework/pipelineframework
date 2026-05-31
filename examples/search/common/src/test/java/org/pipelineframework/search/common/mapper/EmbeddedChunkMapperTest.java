package org.pipelineframework.search.common.mapper;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddedChunkMapperTest {

  @Test
  void mapsDomainToDtoAndBack() {
    EmbeddedChunk chunk = new EmbeddedChunk();
    chunk.docId = UUID.randomUUID();
    chunk.batchIndex = 1;
    chunk.tokenCount = 2;
    chunk.tokens = "alpha beta";
    chunk.tokensHash = "tokens-hash";
    chunk.contentHash = "content-hash";
    chunk.vectorHash = "vector-hash";
    chunk.vectorVersion = "vec-v1";

    EmbeddedChunkDto dto = EmbeddedChunkMapper.INSTANCE.toDto(chunk);
    EmbeddedChunk roundTrip = EmbeddedChunkMapper.INSTANCE.fromDto(dto);

    assertEquals(chunk.docId, roundTrip.docId);
    assertEquals(chunk.batchIndex, roundTrip.batchIndex);
    assertEquals(chunk.vectorHash, roundTrip.vectorHash);
    assertEquals(chunk.vectorVersion, roundTrip.vectorVersion);
  }

  @Test
  void rejectsInvalidDtoBeforeMapping() {
    EmbeddedChunkDto dto = EmbeddedChunkDto.builder()
        .batchIndex(0)
        .tokenCount(1)
        .build();

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> EmbeddedChunkMapper.INSTANCE.fromDto(dto));
    assertTrue(error.getMessage().contains("vectorHash must not be blank"));
  }
}
