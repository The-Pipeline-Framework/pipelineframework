package org.pipelineframework.search.common.mapper;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddedChunkMapperTest {

  private static final EmbeddedChunkMapper MAPPER = Mappers.getMapper(EmbeddedChunkMapper.class);

  @Test
  void mapsDomainToDtoAndBack() {
    EmbeddedChunk chunk = new EmbeddedChunk();
    chunk.docId = UUID.fromString("00000000-0000-0000-0000-000000000001");
    chunk.batchIndex = 1;
    chunk.tokenCount = 2;
    chunk.tokens = "alpha beta";
    chunk.tokensHash = "tokens-hash";
    chunk.contentHash = "content-hash";
    chunk.vectorHash = "vector-hash";
    chunk.vectorVersion = "vec-v1";

    EmbeddedChunkDto dto = MAPPER.toDto(chunk);
    EmbeddedChunk roundTrip = MAPPER.fromDto(dto);

    assertEquals(chunk.docId, roundTrip.docId);
    assertEquals(chunk.batchIndex, roundTrip.batchIndex);
    assertEquals(chunk.vectorHash, roundTrip.vectorHash);
    assertEquals(chunk.vectorVersion, roundTrip.vectorVersion);
    assertEquals(chunk.tokenCount, roundTrip.tokenCount);
    assertEquals(chunk.tokens, roundTrip.tokens);
    assertEquals(chunk.tokensHash, roundTrip.tokensHash);
    assertEquals(chunk.contentHash, roundTrip.contentHash);
  }

  @Test
  void rejectsInvalidDtoBeforeMapping() {
    assertInvalid(validDto().vectorHash(" ").build(), "vectorHash");
    assertInvalid(validDto().docId(null).build(), "docId");
    assertInvalid(validDto().tokens(" ").build(), "tokens");
    assertInvalid(validDto().tokensHash(" ").build(), "tokensHash");
    assertInvalid(validDto().contentHash(" ").build(), "contentHash");
    assertInvalid(validDto().vectorVersion(" ").build(), "vectorVersion");
  }

  private EmbeddedChunkDto.EmbeddedChunkDtoBuilder validDto() {
    return EmbeddedChunkDto.builder()
        .docId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
        .batchIndex(0)
        .tokenCount(1)
        .tokens("alpha")
        .tokensHash("tokens-hash")
        .contentHash("content-hash")
        .vectorHash("vector-hash")
        .vectorVersion("v1");
  }

  private void assertInvalid(EmbeddedChunkDto dto, String expectedMessage) {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> MAPPER.fromDto(dto));
    assertTrue(error.getMessage().contains(expectedMessage));
  }
}
