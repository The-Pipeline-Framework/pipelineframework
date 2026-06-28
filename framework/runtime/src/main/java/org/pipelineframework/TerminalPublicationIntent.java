package org.pipelineframework;

import java.util.Objects;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.controlplane.TerminalPublicationClaim;

/**
 * Immutable intent for one terminal publication side effect.
 */
record TerminalPublicationIntent(
    String kind,
    ClaimedSegment segment,
    Supplier<Uni<Void>> effect
) {

  static final String CHECKPOINT = "checkpoint";
  static final String OBJECT_PUBLISH = "object-publish";

  TerminalPublicationIntent {
    if (kind == null || kind.isBlank()) {
      throw new IllegalArgumentException("kind must not be blank");
    }
    kind = kind.trim();
    Objects.requireNonNull(segment, "segment must not be null");
    Objects.requireNonNull(effect, "effect must not be null");
  }

  Uni<Void> run(SegmentBoundaryLedger ledger, long nowEpochMs) {
    Objects.requireNonNull(ledger, "ledger must not be null");
    ExecutionRecord<Object, Object> record = segment.record();
    return ledger.claimTerminalPublication(record, segment.transitionKey(), kind, nowEpochMs)
        .onItem().transformToUni(claim -> {
          if (!claim.shouldPublish()) {
            return Uni.createFrom().voidItem();
          }
          return effect.get()
              .chain(() -> completeIfTracked(ledger, claim, record, nowEpochMs));
        });
  }

  private Uni<Void> completeIfTracked(
      SegmentBoundaryLedger ledger,
      TerminalPublicationClaim claim,
      ExecutionRecord<Object, Object> record,
      long nowEpochMs) {
    if (claim.status() == TerminalPublicationClaim.Status.UNTRACKED) {
      return Uni.createFrom().voidItem();
    }
    return ledger.completeTerminalPublication(record, segment.transitionKey(), kind, nowEpochMs);
  }
}
