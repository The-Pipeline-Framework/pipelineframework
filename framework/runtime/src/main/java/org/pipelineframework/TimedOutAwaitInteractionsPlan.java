package org.pipelineframework;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.pipelineframework.awaitable.AwaitInteractionRecord;

record TimedOutAwaitInteractionsPlan(List<AwaitInteractionRecord> interactions) {

  TimedOutAwaitInteractionsPlan {
    Objects.requireNonNull(interactions, "interactions must not be null");
    interactions = List.copyOf(interactions);
  }

  static TimedOutAwaitInteractionsPlan from(List<AwaitInteractionRecord> interactions, int limit) {
    Objects.requireNonNull(interactions, "interactions must not be null");
    return new TimedOutAwaitInteractionsPlan(interactions.stream()
        .sorted(Comparator
            .comparingLong(AwaitInteractionRecord::deadlineEpochMs)
            .thenComparing(AwaitInteractionRecord::tenantId)
            .thenComparing(AwaitInteractionRecord::interactionId))
        .limit(Math.max(0, limit))
        .toList());
  }

  boolean empty() {
    return interactions.isEmpty();
  }
}
