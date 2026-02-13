package org.pipelineframework.search.index_document.service;

import java.time.Duration;
import java.util.UUID;

import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.TokenBatch;
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
    assertTrue(ack.success);
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

  private TokenBatch tokenBatch(UUID docId, String tokens, String tokensHash) {
    TokenBatch batch = new TokenBatch();
    batch.docId = docId;
    batch.tokens = tokens;
    batch.tokensHash = tokensHash;
    return batch;
  }
}
