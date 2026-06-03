package org.pipelineframework.search.index_document.service;

import java.time.Duration;
import java.util.UUID;

import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.util.HashingUtils;
import org.pipelineframework.step.NonRetryableException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessIndexDocumentServiceReliabilityTest {

  private IndexFailureParkingLot parkingLot;
  private ProcessIndexDocumentService service;

  @BeforeEach
  void setUp() {
    parkingLot = new IndexFailureParkingLot();
    service = new ProcessIndexDocumentService(
        parkingLot, 3, Duration.ofMillis(1), Duration.ofMillis(5), true);
  }

  @Test
  void transientFailureRecoversWithinRetryBudget() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk chunk = embeddedChunk(docId, "alpha __FAIL_TRANSIENT_2__ beta", "hash-1", "vector-1");

    IndexAck ack = service.process(Multi.createFrom().item(chunk)).await().atMost(Duration.ofSeconds(5));

    assertNotNull(ack);
    assertEquals(docId, ack.docId);
    assertTrue(Boolean.TRUE.equals(ack.getSuccess()));
    assertEquals(0, parkingLot.size(), "No parking expected when transient failure recovers");
  }

  @Test
  void permanentFailureIsParkedAndFailsFast() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk chunk = embeddedChunk(docId, "alpha __FAIL_PERMANENT__ beta", "hash-2", "vector-2");

    NonRetryableException error = assertThrows(
        NonRetryableException.class,
        () -> service.process(Multi.createFrom().item(chunk)).await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("permanent indexing failure marker"));
    assertEquals(1, parkingLot.size(), "Permanent failures must be parked");
    assertEquals(docId.toString(), parkingLot.snapshot().get(0).docId());
  }

  @Test
  void transientFailureExhaustionIsParked() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk chunk = embeddedChunk(docId, "alpha __FAIL_TRANSIENT_99__ beta", "hash-3", "vector-3");

    TransientIndexingException error = assertThrows(
        TransientIndexingException.class,
        () -> service.process(Multi.createFrom().item(chunk)).await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("transient indexing failure"));
    assertEquals(1, parkingLot.size(), "Transient failures exceeding retries must be parked");
    assertEquals("TransientIndexingException", parkingLot.snapshot().get(0).errorType());
  }

  @Test
  void aggregatesFanoutBatchesIntoMeaningfulDocumentSummary() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk batch2 = embeddedChunk(docId, 2, "gamma alpha alpha beta", "hash-c", "vector-c");
    EmbeddedChunk batch0 = embeddedChunk(docId, 0, "alpha beta alpha", "hash-a", "vector-a");
    EmbeddedChunk batch1 = embeddedChunk(docId, 1, "beta delta", "hash-b", "vector-b");

    IndexAck ack =
        service.process(Multi.createFrom().items(batch2, batch0, batch1)).await().atMost(Duration.ofSeconds(5));

    assertNotNull(ack);
    assertEquals(docId, ack.docId);
    assertTrue(Boolean.TRUE.equals(ack.getSuccess()));
    assertEquals(3, ack.getTokenBatchCount());
    assertEquals(4, ack.getUniqueTokenCount());
    assertEquals("alpha", ack.getTopToken());
    assertEquals(HashingUtils.sha256Base64Url("vector-a|vector-b|vector-c"), ack.getTokensHash());
  }

  @Test
  void choosesLexicographicallySmallestTopTokenWhenFrequenciesTie() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk batch0 = embeddedChunk(docId, 0, "alpha beta", "hash-a", "vector-a");
    EmbeddedChunk batch1 = embeddedChunk(docId, 1, "beta alpha", "hash-b", "vector-b");

    IndexAck ack =
        service.process(Multi.createFrom().items(batch0, batch1)).await().atMost(Duration.ofSeconds(5));

    assertNotNull(ack);
    assertEquals("alpha", ack.getTopToken());
  }

  @Test
  void rejectsNegativeBatchIndexBeforeAggregation() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk invalid = embeddedChunk(docId, -1, "alpha beta", "hash-a", "vector-a");

    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> service.process(Multi.createFrom().item(invalid)).await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("invalid embedded chunk metrics for docId"));
  }

  @Test
  void rejectsNonPositiveTokenCountBeforeAggregation() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk invalid = embeddedChunk(docId, 0, "alpha beta", "hash-a", "vector-a");
    invalid.tokenCount = 0;

    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> service.process(Multi.createFrom().item(invalid)).await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("invalid embedded chunk metrics for docId"));
  }

  @Test
  void rejectsMixedDocIdsBeforeAggregation() {
    EmbeddedChunk first = embeddedChunk(UUID.randomUUID(), 0, "alpha beta", "hash-a", "vector-a");
    EmbeddedChunk second = embeddedChunk(UUID.randomUUID(), 1, "gamma delta", "hash-b", "vector-b");

    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> service.process(Multi.createFrom().items(first, second)).await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("all embedded chunks must share the same docId"));
  }

  @Test
  void rejectsMissingVectorMetadataBeforeAggregation() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk invalid = embeddedChunk(docId, 0, "alpha beta", "hash-a", " ");

    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> service.process(Multi.createFrom().item(invalid)).await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("vectorHash"));
  }

  private EmbeddedChunk embeddedChunk(UUID docId, String tokens, String tokensHash, String vectorHash) {
    return embeddedChunk(docId, 0, tokens, tokensHash, vectorHash);
  }

  private EmbeddedChunk embeddedChunk(UUID docId, int batchIndex, String tokens, String tokensHash, String vectorHash) {
    EmbeddedChunk chunk = new EmbeddedChunk();
    chunk.docId = docId;
    chunk.batchIndex = batchIndex;
    chunk.tokens = tokens;
    chunk.tokensHash = tokensHash;
    chunk.tokenCount = tokens == null || tokens.isBlank() ? 0 : tokens.trim().split("\\s+").length;
    chunk.vectorHash = vectorHash;
    chunk.vectorVersion = "vec-v1";
    return chunk;
  }
}
