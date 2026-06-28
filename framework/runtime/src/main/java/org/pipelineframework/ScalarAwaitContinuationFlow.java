package org.pipelineframework;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCompletionMetrics;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

class ScalarAwaitContinuationFlow {

  private static final Logger LOG = Logger.getLogger(ScalarAwaitContinuationFlow.class);

  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder;

  ScalarAwaitContinuationFlow(
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder) {
    this.executionStateStore = executionStateStore;
    this.workDispatcher = workDispatcher;
    this.segmentBoundaryLedger = segmentBoundaryLedger;
    this.lifecycleRecorder = lifecycleRecorder;
  }

  Uni<Void> release(AwaitContinuationPlan.ReleaseScalar plan, long nowEpochMs) {
    AwaitInteractionRecord interaction = plan.interaction();
    AwaitUnitRecord unit = plan.unit();
    return release(
        interaction.tenantId(),
        interaction.executionId(),
        unit.unitId(),
        unit.stepId(),
        interaction.stepIndex(),
        interaction.interactionId(),
        interaction.correlationId(),
        interaction.transportType(),
        interaction.itemIndex(),
        nowEpochMs);
  }

  private Uni<Void> release(
      String tenantId,
      String executionId,
      String awaitUnitId,
      String stepId,
      int stepIndex,
      String interactionId,
      String correlationId,
      String transportType,
      Integer itemIndex,
      long nowEpochMs) {
    return executionStateStore.markAwaitCompleted(
            tenantId,
            executionId,
            awaitUnitId,
            stepIndex + 1,
            nowEpochMs)
        .onItem().transformToUni(updated -> updated
            .map(released -> recordAndEnqueue(
                released,
                awaitUnitId,
                stepId,
                stepIndex,
                interactionId,
                correlationId,
                transportType,
                itemIndex,
                nowEpochMs))
            .orElseGet(() -> enqueueAlreadyReleased(
                tenantId,
                executionId,
                awaitUnitId,
                stepId,
                stepIndex,
                interactionId,
                correlationId,
                transportType,
                itemIndex,
                nowEpochMs)));
  }

  private Uni<Void> enqueueAlreadyReleased(
      String tenantId,
      String executionId,
      String awaitUnitId,
      String stepId,
      int stepIndex,
      String interactionId,
      String correlationId,
      String transportType,
      Integer itemIndex,
      long nowEpochMs) {
    LOG.debugf(
        "Await resume release for execution %s awaitUnitId=%s at stepIndex=%d produced no state update; treating as idempotent no-op",
        executionId,
        awaitUnitId,
        stepIndex);
    return executionStateStore.getExecution(tenantId, executionId)
        .onItem().transformToUni(current -> {
          if (current.isEmpty() || !releasedForStep(current.get(), stepIndex + 1)) {
            return Uni.createFrom().voidItem();
          }
          return recordAndEnqueue(
              current.get(),
              awaitUnitId,
              stepId,
              stepIndex,
              interactionId,
              correlationId,
              transportType,
              itemIndex,
              nowEpochMs);
        });
  }

  private Uni<Void> recordAndEnqueue(
      ExecutionRecord<Object, Object> released,
      String awaitUnitId,
      String stepId,
      int stepIndex,
      String interactionId,
      String correlationId,
      String transportType,
      Integer itemIndex,
      long nowEpochMs) {
    return segmentBoundaryLedger.get().recordContinuationSegmentCreated(
            released,
            awaitUnitId,
            SegmentBoundaryLedger.segmentId(released),
            stepIndex + 1,
            -1,
            released.inputPayload(),
            nowEpochMs)
        .chain(() -> workDispatcher.enqueueNow(new ExecutionWorkItem(
            released.tenantId(),
            released.executionId())))
        .invoke(() -> {
          LOG.infof(
              "Resuming async execution %s from awaitUnitId=%s at nextStepIndex=%d",
              released.executionId(),
              awaitUnitId,
              released.currentStepIndex());
          AwaitReplayLifecycleEvent lifecycleEvent = new AwaitReplayLifecycleEvent(
              AwaitReplayLifecycleEvent.RESUME_RELEASED,
              released.executionId(),
              awaitUnitId,
              stepId,
              stepIndex,
              released.status().name(),
              interactionId,
              correlationId,
              transportType,
              itemIndex,
              null,
              null,
              null);
          lifecycleRecorder.accept(lifecycleEvent);
          AwaitCompletionMetrics.recordResumeReleased(lifecycleEvent);
        })
        .replaceWithVoid();
  }

  private static boolean releasedForStep(
      ExecutionRecord<Object, Object> record,
      int stepIndex) {
    return record != null
        && record.status() == ExecutionStatus.QUEUED
        && record.currentStepIndex() == stepIndex;
  }
}
