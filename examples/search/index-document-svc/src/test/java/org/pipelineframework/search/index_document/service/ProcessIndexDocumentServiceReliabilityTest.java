package org.pipelineframework.search.index_document.service;

import java.time.Duration;
import java.util.UUID;

import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.TokenBatch;
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
    TokenBatch batch = tokenBatch(docId, "alpha __FAIL_TRANSIENT_2__ beta", "hash-1");

    IndexAck ack = service.process(Multi.createFrom().item(batch)).await().atMost(Duration.ofSeconds(5));

    assertNotNull(ack);
    assertEquals(docId, ack.docId);
    assertTrue(Boolean.TRUE.equals(ack.getSuccess()));
    assertEquals(0, parkingLot.size(), "No parking expected when transient failure recovers");
  }

  @Test
  void permanentFailureIsParkedAndFailsFast() {
    UUID docId = UUID.randomUUID();
    TokenBatch batch = tokenBatch(docId, "alpha __FAIL_PERMANENT__ beta", "hash-2");

    NonRetryableException error = assertThrows(
        NonRetryableException.class,
        () -> service.process(Multi.createFrom().item(batch)).await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("permanent indexing failure marker"));
    assertEquals(1, parkingLot.size(), "Permanent failures must be parked");
    assertEquals(docId.toString(), parkingLot.snapshot().get(0).docId());
  }

  @Test
  void transientFailureExhaustionIsParked() {
    UUID docId = UUID.randomUUID();
    TokenBatch batch = tokenBatch(docId, "alpha __FAIL_TRANSIENT_99__ beta", "hash-3");

    TransientIndexingException error = assertThrows(
        TransientIndexingException.class,
        () -> service.process(Multi.createFrom().item(batch)).await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("transient indexing failure"));
    assertEquals(1, parkingLot.size(), "Transient failures exceeding retries must be parked");
    assertEquals("TransientIndexingException", parkingLot.snapshot().get(0).errorType());
  }

  @Test
  void aggregatesFanoutBatchesIntoMeaningfulDocumentSummary() {
    UUID docId = UUID.randomUUID();
    TokenBatch batch2 = tokenBatch(docId, 2, "gamma alpha alpha beta", "hash-c");
    TokenBatch batch0 = tokenBatch(docId, 0, "alpha beta alpha", "hash-a");
    TokenBatch batch1 = tokenBatch(docId, 1, "beta delta", "hash-b");

    IndexAck ack =
        service.process(Multi.createFrom().items(batch2, batch0, batch1)).await().atMost(Duration.ofSeconds(5));

    assertNotNull(ack);
    assertEquals(docId, ack.docId);
    assertTrue(Boolean.TRUE.equals(ack.getSuccess()));
    assertEquals(3, ack.getTokenBatchCount());
    assertEquals(4, ack.getUniqueTokenCount());
    assertEquals("alpha", ack.getTopToken());
    assertEquals(HashingUtils.sha256Base64Url("hash-a|hash-b|hash-c"), ack.getTokensHash());
  }

  @Test
  void choosesLexicographicallySmallestTopTokenWhenFrequenciesTie() {
    UUID docId = UUID.randomUUID();
    TokenBatch batch0 = tokenBatch(docId, 0, "alpha beta", "hash-a");
    TokenBatch batch1 = tokenBatch(docId, 1, "beta alpha", "hash-b");

    IndexAck ack =
        service.process(Multi.createFrom().items(batch0, batch1)).await().atMost(Duration.ofSeconds(5));

    assertNotNull(ack);
    assertEquals("alpha", ack.getTopToken());
  }

  private TokenBatch tokenBatch(UUID docId, String tokens, String tokensHash) {
    return tokenBatch(docId, 0, tokens, tokensHash);
  }

  private TokenBatch tokenBatch(UUID docId, int batchIndex, String tokens, String tokensHash) {
    TokenBatch batch = new TokenBatch();
    batch.docId = docId;
    batch.batchIndex = batchIndex;
    batch.tokens = tokens;
    batch.tokensHash = tokensHash;
    batch.tokenCount = tokens == null || tokens.isBlank() ? 0 : tokens.trim().split("\\s+").length;
    return batch;
  }
}
