package org.pipelineframework;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

/**
 * Interprets immutable segment commit plans against the existing projection stores.
 */
class SegmentCommitEffects {

  private static final Logger LOG = Logger.getLogger(SegmentCommitEffects.class);

  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final DeadLetterPublisher deadLetterPublisher;
  private final AwaitCoordinator awaitCoordinator;
  private final ExecutionFailureHandler executionFailureHandler;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final AwaitContinuations awaitContinuations;
  private final TerminalPublicationBoundary terminalPublicationBoundary;
  private final Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder;

  SegmentCommitEffects(
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      DeadLetterPublisher deadLetterPublisher,
      AwaitCoordinator awaitCoordinator,
      ExecutionFailureHandler executionFailureHandler,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      AwaitContinuations awaitContinuations,
      TerminalPublicationBoundary terminalPublicationBoundary,
      Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder) {
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.deadLetterPublisher = Objects.requireNonNull(deadLetterPublisher, "deadLetterPublisher must not be null");
    this.awaitCoordinator = Objects.requireNonNull(awaitCoordinator, "awaitCoordinator must not be null");
    this.executionFailureHandler = Objects.requireNonNull(executionFailureHandler, "executionFailureHandler must not be null");
    this.segmentBoundaryLedger = Objects.requireNonNull(segmentBoundaryLedger, "segmentBoundaryLedger must not be null");
    this.awaitContinuations = Objects.requireNonNull(awaitContinuations, "awaitContinuations must not be null");
    this.terminalPublicationBoundary = Objects.requireNonNull(
        terminalPublicationBoundary,
        "terminalPublicationBoundary must not be null");
    this.lifecycleRecorder = Objects.requireNonNull(lifecycleRecorder, "lifecycleRecorder must not be null");
  }

  Uni<Void> commit(
      SegmentCommitPlan plan,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return switch (plan) {
      case CompletedSegment completed -> commitCompleted(completed);
      case SuspendedSegment suspended -> commitSuspended(suspended, itemContinuationHandler);
      case FailedSegment failed -> fail(failed.segment(), failed.failure());
    };
  }

  Uni<Void> fail(ClaimedSegment segment, Throwable failure) {
    return executionFailureHandler.handleExecutionFailure(
        segment.record(),
        segment.transitionKey(),
        failure,
        executionStateStore,
        workDispatcher,
        deadLetterPublisher);
  }

  private Uni<Void> commitCompleted(CompletedSegment completed) {
    ClaimedSegment segment = completed.segment();
    long nowEpochMs = System.currentTimeMillis();
    return segmentBoundaryLedger.get()
        .recordSegmentCompleted(segment.record(), segment.transitionKey(), completed.result(), nowEpochMs)
        .chain(() -> terminalPublicationBoundary.publishBeforeSuccess(completed, nowEpochMs))
        .chain(() -> executionStateStore.markSucceeded(
            segment.record().tenantId(),
            segment.record().executionId(),
            segment.record().version(),
            segment.transitionKey(),
            completed.outputItems(),
            nowEpochMs))
        .onItem().transformToUni(updated -> updated
            .map(succeeded -> segmentBoundaryLedger.get().recordRunSucceeded(
                succeeded,
                completed.outputItems(),
                nowEpochMs))
            .orElseGet(() -> Uni.createFrom().failure(successCommitFailure(completed))))
        .replaceWithVoid();
  }

  private Uni<Void> commitSuspended(
      SuspendedSegment suspended,
      AwaitItemContinuationHandler itemContinuationHandler) {
    ClaimedSegment segment = suspended.segment();
    long nowEpochMs = System.currentTimeMillis();
    return executionStateStore.markWaitingExternal(
            segment.record().tenantId(),
            segment.record().executionId(),
            segment.record().version(),
            segment.transitionKey(),
            suspended.suspension().unitId(),
            suspended.suspension().stepIndex(),
            nowEpochMs)
        .onItem().transformToUni(updated -> {
          if (updated.isEmpty()) {
            return Uni.createFrom().failure(waitingExternalFailure(suspended));
          }
          return awaitCoordinator.importSuspension(suspended.suspension())
              .chain(() -> afterWaitingExternal(suspended, updated.get(), itemContinuationHandler, nowEpochMs));
        });
  }

  private Uni<Void> afterWaitingExternal(
      SuspendedSegment suspended,
      ExecutionRecord<Object, Object> waiting,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    ClaimedSegment segment = suspended.segment();
    LOG.infof(
        "Execution %s persisted WAITING_EXTERNAL at stepIndex=%d awaitUnitId=%s awaitInteractions=%d",
        segment.record().executionId(),
        suspended.suspension().stepIndex(),
        suspended.suspension().unitId(),
        suspended.suspension().interactions().size());
    lifecycleRecorder.accept(new AwaitReplayLifecycleEvent(
        AwaitReplayLifecycleEvent.EXECUTION_WAITING,
        segment.record().executionId(),
        suspended.suspension().unitId(),
        null,
        suspended.suspension().stepIndex(),
        ExecutionStatus.WAITING_EXTERNAL.name(),
        null,
        null,
        null,
        null,
        null,
        null,
        null));
    return segmentBoundaryLedger.get()
        .recordSegmentSuspended(segment.record(), segment.transitionKey(), suspended.suspension(), nowEpochMs)
        .chain(() -> awaitContinuations.afterParentWaiting(
            waiting,
            suspended.suspension(),
            nowEpochMs,
            itemContinuationHandler));
  }

  private IllegalStateException waitingExternalFailure(SuspendedSegment suspended) {
    return new IllegalStateException(
        "Failed to persist WAITING_EXTERNAL state for execution "
            + suspended.segment().record().executionId()
            + " at step "
            + suspended.suspension().stepIndex()
            + " (expectedVersion="
            + suspended.segment().record().version()
            + ", awaitUnitId="
            + suspended.suspension().unitId()
            + ")");
  }

  private IllegalStateException successCommitFailure(CompletedSegment completed) {
    return new IllegalStateException(
        "Failed to persist SUCCEEDED state for execution "
            + completed.segment().record().executionId()
            + " (expectedVersion="
            + completed.segment().record().version()
            + ", transitionKey="
            + completed.segment().transitionKey()
            + ")");
  }
}
