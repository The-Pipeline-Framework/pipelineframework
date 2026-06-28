package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

/**
 * Facade for durable continuations that create the future beginning after an await boundary.
 */
class AwaitContinuations {

  static final AwaitItemContinuationHandler NOOP_ITEM_CONTINUATION_HANDLER =
      new AwaitItemContinuationHandler() {
        @Override
        public Uni<Void> continueAwaitItem(
            AwaitInteractionRecord record,
            AwaitUnitRecord unit,
            int nextStepIndex,
            Optional<ExecutionRecord<Object, Object>> parent,
            long nowEpochMs) {
          return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> releaseAwaitParentIfReady(
            ExecutionRecord<Object, Object> parent,
            AwaitUnitRecord unit,
            int nextStepIndex,
            long nowEpochMs) {
          return Uni.createFrom().voidItem();
        }
      };

  private final AwaitContinuationPlanner planner;
  private final AwaitCoordinator awaitCoordinator;
  private final ScalarAwaitContinuationFlow scalarFlow;
  private final ItemizedAwaitContinuationFlow itemizedFlow;

  AwaitContinuations(
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitCoordinator awaitCoordinator,
      TransitionWorkerExecutor transitionWorkerExecutor,
      ScheduledExecutorService queueSweepExecutor,
      Supplier<Duration> saturatedDelay,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder) {
    this.planner = new AwaitContinuationPlanner();
    this.awaitCoordinator = awaitCoordinator;
    ItemContinuationClaims claims = new ItemContinuationClaims();
    this.scalarFlow = new ScalarAwaitContinuationFlow(
        executionStateStore,
        workDispatcher,
        segmentBoundaryLedger,
        lifecycleRecorder);
    this.itemizedFlow = new ItemizedAwaitContinuationFlow(
        executionStateStore,
        workDispatcher,
        awaitCoordinator,
        transitionWorkerExecutor,
        queueSweepExecutor,
        saturatedDelay,
        segmentBoundaryLedger,
        lifecycleRecorder,
        planner,
        claims);
  }

  Uni<AwaitCompletionResult> afterRecordedCompletion(
      AwaitCompletionResult result,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    AwaitInteractionRecord record = result.record();
    if (AwaitContinuationPlanner.usesItemContinuations(record, unit)) {
      return itemizedFlow.afterRecordedCompletion(
              record,
              unit,
              itemContinuationHandler,
              nowEpochMs)
          .replaceWith(result);
    }
    AwaitContinuationPlan plan = planner.afterRecordedCompletion(record, unit, Optional.empty());
    if (plan instanceof AwaitContinuationPlan.ReleaseScalar scalar) {
      return scalarFlow.release(scalar, nowEpochMs).replaceWith(result);
    }
    return Uni.createFrom().item(result);
  }

  Uni<Void> afterParentWaiting(
      ExecutionRecord<Object, Object> record,
      TransitionAwaitSuspension suspended,
      long nowEpochMs,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return awaitCoordinator.getUnit(record.tenantId(), suspended.unitId())
        .onItem().transformToUni(unit -> {
          if (unit == null) {
            return Uni.createFrom().voidItem();
          }
          AwaitContinuationPlan plan = planner.afterParentWaiting(record, unit, suspended.stepIndex());
          if (plan instanceof AwaitContinuationPlan.ReleaseScalar scalar) {
            return scalarFlow.release(scalar, nowEpochMs);
          }
          return itemizedFlow.afterParentWaiting(
              record,
              unit,
              suspended.stepIndex(),
              itemContinuationHandler,
              nowEpochMs);
        });
  }

  Uni<Void> captureItemContinuationOutput(
      AwaitInteractionRecord interaction,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      ExecutionInputSnapshot continuationInput,
      List<?> segmentOutputs,
      long nowEpochMs) {
    return itemizedFlow.captureOutput(
        interaction,
        unit,
        aggregateStepIndex,
        continuationInput,
        segmentOutputs,
        nowEpochMs);
  }

  Uni<Void> releaseParentIfReady(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      long nowEpochMs) {
    return itemizedFlow.releaseParentIfReady(parent, unit, aggregateStepIndex, nowEpochMs);
  }
}
