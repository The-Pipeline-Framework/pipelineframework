package org.pipelineframework;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimedOutAwaitInteractionsPlanTest {

  @Test
  void interactionsAreSortedBeforeLimitIsApplied() {
    AwaitInteractionRecord later = interaction("tenant-b", "interaction-b", 20L);
    AwaitInteractionRecord firstTie = interaction("tenant-a", "interaction-c", 10L);
    AwaitInteractionRecord secondTie = interaction("tenant-b", "interaction-a", 10L);

    TimedOutAwaitInteractionsPlan plan = TimedOutAwaitInteractionsPlan.from(
        List.of(later, secondTie, firstTie),
        2);

    assertEquals(List.of(firstTie, secondTie), plan.interactions());
  }

  @Test
  void interactionsAreDefensivelyCopied() {
    List<AwaitInteractionRecord> records = new java.util.ArrayList<>();
    AwaitInteractionRecord interaction = interaction("tenant-a", "interaction-a", 10L);
    records.add(interaction);

    TimedOutAwaitInteractionsPlan plan = new TimedOutAwaitInteractionsPlan(records);
    records.clear();

    assertEquals(List.of(interaction), plan.interactions());
    assertThrows(UnsupportedOperationException.class, () -> plan.interactions().clear());
  }

  private static AwaitInteractionRecord interaction(String tenantId, String interactionId, long deadlineEpochMs) {
    return new AwaitInteractionRecord(
        tenantId,
        "exec-1",
        "AwaitStep",
        2,
        String.class.getName(),
        interactionId,
        "corr-" + interactionId,
        "cause-" + interactionId,
        "idem-" + interactionId,
        1L,
        AwaitInteractionStatus.DISPATCHED,
        "request",
        null,
        "unit-1",
        null,
        "user-1",
        null,
        null,
        "kafka",
        java.util.Map.of(),
        deadlineEpochMs,
        1L,
        1L,
        99999999L);
  }
}
