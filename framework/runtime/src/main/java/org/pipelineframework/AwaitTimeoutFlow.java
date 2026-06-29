package org.pipelineframework;

import java.util.Objects;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

class AwaitTimeoutFlow {

  private final AwaitCoordinator awaitCoordinator;
  private final ExecutionStateStore executionStateStore;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;

  AwaitTimeoutFlow(
      AwaitCoordinator awaitCoordinator,
      ExecutionStateStore executionStateStore,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger) {
    this.awaitCoordinator = Objects.requireNonNull(awaitCoordinator, "awaitCoordinator must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.segmentBoundaryLedger =
        Objects.requireNonNull(segmentBoundaryLedger, "segmentBoundaryLedger must not be null");
  }

  Uni<Void> sweepTimedOut(long nowEpochMs, int limit) {
    return awaitCoordinator.findTimedOut(nowEpochMs, limit)
        .onItem().transform(records -> TimedOutAwaitInteractionsPlan.from(records, limit))
        .onItem().transformToUni(plan -> {
          if (plan.empty()) {
            return Uni.createFrom().voidItem();
          }
          return Multi.createFrom().iterable(plan.interactions())
              .onItem().transformToUniAndConcatenate(record -> admitTimeout(record, nowEpochMs))
              .collect().asList()
              .replaceWithVoid();
        });
  }

  private Uni<Void> admitTimeout(AwaitInteractionRecord interaction, long nowEpochMs) {
    return awaitCoordinator.markTimedOut(interaction, nowEpochMs)
        .onItem().transformToUni(updated -> updated.map(record ->
                segmentBoundaryLedger.get().recordInteractionTimedOut(record, nowEpochMs)
                    .chain(() -> executionStateStore.getExecution(record.tenantId(), record.executionId()))
                    .onItem().transform(parent -> AwaitTimeoutPlan.from(record, parent))
                    .onItem().transformToUni(plan -> plan.failParent()
                        ? failParent(plan, nowEpochMs)
                        : Uni.createFrom().voidItem()))
            .orElseGet(() -> Uni.createFrom().voidItem()));
  }

  private Uni<Void> failParent(AwaitTimeoutPlan plan, long nowEpochMs) {
    var parent = plan.parent().orElseThrow();
    return executionStateStore.markTerminalFailure(
            parent.tenantId(),
            parent.executionId(),
            parent.version(),
            ExecutionStatus.FAILED,
            plan.transitionKey().orElseThrow(),
            plan.errorCode(),
            plan.errorMessage(),
            nowEpochMs)
        .onItem().transformToUni(updated -> updated
            .map(failed -> segmentBoundaryLedger.get().recordRunFailed(
                failed,
                plan.errorCode(),
                plan.errorMessage(),
                nowEpochMs))
            .orElseGet(() -> Uni.createFrom().voidItem()))
        .replaceWithVoid();
  }
}
