package org.pipelineframework.search.embed_content.service;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.util.HashingUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessEmbedContentServiceTest {

  private static final Duration TEST_TIMEOUT = Duration.ofSeconds(1);

  @Test
  void embedsTokenBatchWithDeterministicVectorHashAndStableIdentity() {
    ProcessEmbedContentService service = new ProcessEmbedContentService(0, "vec-v2");
    TokenBatch input = tokenBatch(UUID.randomUUID(), 2, "alpha beta", "tokens-hash");

    EmbeddedChunk output = service.process(input).await().atMost(TEST_TIMEOUT);

    assertEquals(input.docId, output.docId);
    assertEquals(input.batchIndex, output.batchIndex);
    assertEquals(input.tokenCount, output.tokenCount);
    assertEquals(input.tokens, output.tokens);
    assertEquals(input.tokensHash, output.tokensHash);
    assertEquals(input.contentHash, output.contentHash);
    assertEquals("vec-v2", output.vectorVersion);
    assertEquals(expectedVectorHash(input, "vec-v2"), output.vectorHash);
    assertNotNull(output.embeddedAt);
  }

  @Test
  void normalizesDelayAndVectorVersionConfigWithoutTimingAssertions() {
    ProcessEmbedContentService service = new ProcessEmbedContentService(-10, "  ");

    assertEquals(0, service.getDelayMs());
    assertEquals("v1", service.getVectorVersion());
  }

  @Test
  void failsForNullOrInvalidInput() {
    ProcessEmbedContentService service = new ProcessEmbedContentService(0, "v1");
    TokenBatch missingDoc = tokenBatch(null, 0, "alpha", "hash");
    TokenBatch invalidBatch = tokenBatch(UUID.randomUUID(), -1, "alpha", "hash");
    TokenBatch missingTokens = tokenBatch(UUID.randomUUID(), 0, " ", "hash");
    missingTokens.tokenCount = 1;

    assertFailure(service, null, "token batch is required");
    assertFailure(service, missingDoc, "docId is required");
    assertFailure(service, invalidBatch, "batchIndex must be >= 0");
    assertFailure(service, missingTokens, "tokens are required");
  }

  @Test
  void vectorHashChangesWithVectorVersion() {
    TokenBatch input = tokenBatch(UUID.randomUUID(), 0, "alpha beta", "tokens-hash");
    EmbeddedChunk v1 = new ProcessEmbedContentService(0, "v1").process(input).await().atMost(TEST_TIMEOUT);
    EmbeddedChunk v2 = new ProcessEmbedContentService(0, "v2").process(input).await().atMost(TEST_TIMEOUT);

    assertTrue(!v1.vectorHash.equals(v2.vectorHash));
  }

  private void assertFailure(ProcessEmbedContentService service, TokenBatch input, String expectedMessage) {
    Throwable error = assertThrows(Throwable.class, () -> service.process(input).await().atMost(TEST_TIMEOUT));
    assertTrue(rootCauseMessage(error).contains(expectedMessage));
  }

  private TokenBatch tokenBatch(UUID docId, int batchIndex, String tokens, String tokensHash) {
    TokenBatch batch = new TokenBatch();
    batch.docId = docId;
    batch.batchIndex = batchIndex;
    batch.tokenCount = tokens == null || tokens.isBlank() ? 0 : tokens.trim().split("\\s+").length;
    batch.tokens = tokens;
    batch.tokensHash = tokensHash;
    batch.contentHash = "content-hash";
    return batch;
  }

  private String expectedVectorHash(TokenBatch input, String vectorVersion) {
    return HashingUtils.sha256Base64Url(String.join("|",
        vectorVersion,
        input.docId.toString(),
        input.batchIndex.toString(),
        input.tokensHash.trim(),
        input.tokens.trim()));
  }

  private String rootCauseMessage(Throwable error) {
    Throwable cursor = error;
    while (cursor.getCause() != null) {
      cursor = cursor.getCause();
    }
    return cursor.getMessage() == null ? "" : cursor.getMessage();
  }
}
