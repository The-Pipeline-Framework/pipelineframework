package org.pipelineframework;

import java.time.Duration;
import java.util.UUID;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.WorkDispatcher;

/** Processes one item continuation through the normal dispatcher using interaction-row leases. */
final class DurableAwaitItemContinuationFlow {

  private static final long LEASE_MS = 30_000L;
  private static final Duration RETRY_DELAY = Duration.ofSeconds(1);

  private final AwaitCoordinator awaitCoordinator;
  private final WorkDispatcher workDispatcher;
  private final ExecutionStateStore executionStateStore;

  DurableAwaitItemContinuationFlow(
      AwaitCoordinator awaitCoordinator, WorkDispatcher workDispatcher, ExecutionStateStore executionStateStore) {
    this.awaitCoordinator = awaitCoordinator;
    this.workDispatcher = workDispatcher;
    this.executionStateStore = executionStateStore;
  }

  Uni<Void> process(ExecutionWorkItem workItem, AwaitItemContinuationHandler handler) {
    long nowEpochMs = System.currentTimeMillis();
    String leaseOwner = "await-continuation:" + UUID.randomUUID();
    return awaitCoordinator.claimItemContinuation(
        workItem.tenantId(), workItem.awaitInteractionId(), leaseOwner, nowEpochMs, LEASE_MS)
        .onItem().transformToUni(claimed -> claimed
            .map(record -> executeClaimed(record, handler, nowEpochMs))
            .orElseGet(() -> Uni.createFrom().voidItem()));
  }

  private Uni<Void> executeClaimed(
      AwaitInteractionRecord interaction,
      AwaitItemContinuationHandler handler,
      long nowEpochMs) {
    return awaitCoordinator.getUnit(interaction.tenantId(), interaction.unitId())
        .onItem().transformToUni(unit -> executionStateStore.getExecution(
            interaction.tenantId(), interaction.executionId())
            .onItem().transformToUni(parent -> handler.continueDurableAwaitItem(
            interaction, unit, interaction.stepIndex() + 1, parent, nowEpochMs)
            .onItem().transformToUni(output -> interaction.streamRegionId().isBlank()
                ? awaitCoordinator.completeItemContinuation(interaction, output, System.currentTimeMillis())
                : awaitCoordinator.completeItemContinuationAndReleaseStreamCredit(
                    interaction, leaseOwner(interaction), output, System.currentTimeMillis()))
            .onItem().transformToUni(applied -> applied
                .map(record -> enqueueStreamRegionIfLinked(record))
                .orElseGet(() -> retry(interaction, nowEpochMs)))
            .onFailure().recoverWithUni(failure -> retry(interaction, System.currentTimeMillis()))));
  }

  private Uni<Void> enqueueStreamRegionIfLinked(AwaitInteractionRecord interaction) {
    if (interaction.streamRegionId().isBlank()) {
      return Uni.createFrom().voidItem();
    }
    return workDispatcher.enqueueNow(ExecutionWorkItem.streamRegion(
            interaction.tenantId(), interaction.executionId(), interaction.streamRegionId()))
        .onFailure().recoverWithUni(ignored -> Uni.createFrom().voidItem());
  }

  private Uni<Void> retry(AwaitInteractionRecord interaction, long nowEpochMs) {
    long due = nowEpochMs + RETRY_DELAY.toMillis();
    return awaitCoordinator.retryItemContinuation(interaction, due, nowEpochMs)
        .onItem().transformToUni(rescheduled -> rescheduled
            .map(record -> workDispatcher.enqueueDelayed(
                ExecutionWorkItem.awaitContinuation(record.tenantId(), record.executionId(), record.interactionId()),
                RETRY_DELAY))
            .orElseGet(() -> Uni.createFrom().voidItem()));
  }

  private static String leaseOwner(AwaitInteractionRecord interaction) {
    if (interaction.continuationLeaseOwner().isBlank()) {
      throw new IllegalStateException("Claimed await continuation has no lease owner");
    }
    return interaction.continuationLeaseOwner();
  }
}
