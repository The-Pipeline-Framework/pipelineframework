package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCompletionMetrics;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

class ItemizedAwaitContinuationFlow {

  private static final Logger LOG = Logger.getLogger(ItemizedAwaitContinuationFlow.class);
  private static final int MAX_ATTEMPTS = 3;
  private static final long RETRY_BASE_MS = 100L;

  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final AwaitCoordinator awaitCoordinator;
  private final TransitionWorkerExecutor transitionWorkerExecutor;
  private final ScheduledExecutorService queueSweepExecutor;
  private final Supplier<Duration> saturatedDelay;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder;
  private final AwaitContinuationPlanner planner;
  private final ItemContinuationClaims claims;

  ItemizedAwaitContinuationFlow(
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitCoordinator awaitCoordinator,
      TransitionWorkerExecutor transitionWorkerExecutor,
      ScheduledExecutorService queueSweepExecutor,
      Supplier<Duration> saturatedDelay,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder,
      AwaitContinuationPlanner planner,
      ItemContinuationClaims claims) {
    this.executionStateStore = executionStateStore;
    this.workDispatcher = workDispatcher;
    this.awaitCoordinator = awaitCoordinator;
    this.transitionWorkerExecutor = transitionWorkerExecutor;
    this.queueSweepExecutor = queueSweepExecutor;
    this.saturatedDelay = saturatedDelay;
    this.segmentBoundaryLedger = segmentBoundaryLedger;
    this.lifecycleRecorder = lifecycleRecorder;
    this.planner = planner;
    this.claims = claims;
  }

  Uni<Void> afterRecordedCompletion(
      AwaitInteractionRecord record,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    if (record == null || unit == null || !unit.dispatchComplete()) {
      AwaitCompletionMetrics.recordEarlyCompletionHeld(record, unit);
      return Uni.createFrom().voidItem();
    }
    Uni<Optional<ExecutionRecord<Object, Object>>> parentLookup =
        executionStateStore.getExecution(record.tenantId(), record.executionId());
    if (parentLookup == null) {
      AwaitCompletionMetrics.recordEarlyCompletionHeld(record, unit);
      return Uni.createFrom().voidItem();
    }
    return parentLookup
        .onItem().transform(parent -> planner.afterRecordedCompletion(record, unit, parent))
        .onItem().transformToUni(plan -> interpretAfterRecordedCompletion(plan, itemContinuationHandler, nowEpochMs));
  }

  Uni<Void> afterParentWaiting(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int suspendedStepIndex,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    AwaitContinuationPlan plan = planner.afterParentWaiting(parent, unit, suspendedStepIndex);
    if (plan instanceof AwaitContinuationPlan.DispatchItemContinuations dispatch) {
      Uni<Void> dispatched = dispatchCompletedItemContinuations(
          dispatch.parent(),
          dispatch.unit(),
          itemContinuationHandler,
          nowEpochMs);
      return unit.status() == AwaitUnitStatus.COMPLETED
          ? dispatched.chain(() -> itemContinuationHandler.releaseAwaitParentIfReady(
              dispatch.parent(),
              dispatch.unit(),
              dispatch.nextStepIndex(),
              nowEpochMs))
          : dispatched;
    }
    return Uni.createFrom().voidItem();
  }

  Uni<Void> captureOutput(
      AwaitInteractionRecord interaction,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      ExecutionInputSnapshot continuationInput,
      List<?> segmentOutputs,
      long nowEpochMs) {
    if (!AwaitContinuationPlanner.usesItemContinuations(interaction, unit)) {
      return Uni.createFrom().voidItem();
    }
    return executionStateStore.getExecution(interaction.tenantId(), interaction.executionId())
        .onItem().transformToUni(parent -> {
          if (parent.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          AwaitContinuationPlan.RecordItemOutput plan =
              (AwaitContinuationPlan.RecordItemOutput) planner.recordItemOutput(
                  parent.get(),
                  unit,
                  interaction,
                  aggregateStepIndex,
                  continuationInput,
                  segmentOutputs);
          return captureOutput(plan, nowEpochMs);
        });
  }

  Uni<Void> releaseParentIfReady(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      long nowEpochMs) {
    if (parent == null || unit == null || !AwaitContinuationPlanner.usesItemContinuations(unit)
        || !unit.dispatchComplete() || unit.expectedItemCount() == null) {
      return Uni.createFrom().voidItem();
    }
    return Multi.createFrom().range(0, unit.expectedItemCount())
        .onItem().transformToUniAndConcatenate(index ->
            executionStateStore.getExecutionByKey(
                parent.tenantId(),
                ItemContinuationKey.from(parent, unit, index).childExecutionKey()))
        .collect().asList()
        .onItem().transformToUni(children -> {
          AwaitContinuationPlan plan = planner.releaseItemizedParent(parent, unit, aggregateStepIndex, children);
          if (plan instanceof AwaitContinuationPlan.ReleaseItemizedParent release) {
            return releaseParent(release.release(), nowEpochMs);
          }
          return Uni.createFrom().voidItem();
        });
  }

  private Uni<Void> interpretAfterRecordedCompletion(
      AwaitContinuationPlan plan,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    if (plan instanceof AwaitContinuationPlan.HoldCompletion held) {
      AwaitCompletionMetrics.recordEarlyCompletionHeld(held.interaction(), held.unit());
      return Uni.createFrom().voidItem();
    }
    if (plan instanceof AwaitContinuationPlan.DispatchItemContinuations dispatch) {
      return dispatchCompletedItemContinuations(
          dispatch.parent(),
          dispatch.unit(),
          itemContinuationHandler,
          nowEpochMs);
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> dispatchCompletedItemContinuations(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    if (parent == null || unit == null || !AwaitContinuationPlanner.usesItemContinuations(unit) || !unit.dispatchComplete()
        || itemContinuationHandler == AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER) {
      return Uni.createFrom().voidItem();
    }
    return awaitCoordinator.findByUnit(parent.tenantId(), unit.unitId())
        .onItem().invoke(records -> records.stream()
            .filter(record -> record.itemInteraction()
                && record.itemIndex() != null
                && record.status() == AwaitInteractionStatus.COMPLETED)
            .forEach(record -> dispatchItemContinuation(
                record,
                unit,
                itemContinuationHandler,
                nowEpochMs)))
        .replaceWithVoid();
  }

  private void dispatchItemContinuation(
      AwaitInteractionRecord record,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    planner.validateItemIndex(record, unit);
    ItemContinuationKey key = new ItemContinuationKey(
        record.tenantId(),
        record.executionId(),
        record.executionId(),
        record.unitId(),
        record.itemIndex());
    if (!claims.claimDispatch(key)) {
      return;
    }
    dispatchItemContinuationAttempt(record, unit, itemContinuationHandler, nowEpochMs, key, 1);
  }

  private void dispatchItemContinuationAttempt(
      AwaitInteractionRecord record,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs,
      ItemContinuationKey key,
      int attempt) {
    Optional<TransitionWorkerExecutor.TransitionAdmission> admission = transitionWorkerExecutor.tryAdmit();
    if (admission.isEmpty()) {
      queueSweepExecutor.schedule(
          () -> dispatchItemContinuationAttempt(record, unit, itemContinuationHandler, nowEpochMs, key, attempt),
          saturatedDelay.get().toMillis(),
          TimeUnit.MILLISECONDS);
      return;
    }
    TransitionWorkerExecutor.TransitionAdmission permit = admission.get();
    try {
      Infrastructure.getDefaultExecutor().execute(() ->
          executionStateStore
              .getExecution(record.tenantId(), record.executionId())
              .onItem().transformToUni(parent -> {
                if (!planner.itemContinuationReady(parent, unit)) {
                  return Uni.createFrom().item(false);
                }
                return itemContinuationHandler.continueAwaitItem(
                    record,
                    unit,
                    record.stepIndex() + 1,
                    parent,
                    nowEpochMs)
                    .replaceWith(true);
              })
              .subscribe().with(
                  dispatched -> {
                    permit.close();
                    if (!Boolean.TRUE.equals(dispatched)) {
                      claims.releaseDispatch(key);
                    }
                  },
                  failure -> {
                    permit.close();
                    if (attempt < MAX_ATTEMPTS) {
                      long retryDelayMs = retryDelayMs(attempt);
                      LOG.warnf(
                          failure,
                          "Retrying await item continuation tenant=%s executionId=%s unitId=%s interactionId=%s itemIndex=%s attempt=%d delayMs=%d",
                          record.tenantId(),
                          record.executionId(),
                          record.unitId(),
                          record.interactionId(),
                          record.itemIndex(),
                          attempt + 1,
                          retryDelayMs);
                      queueSweepExecutor.schedule(
                          () -> dispatchItemContinuationAttempt(
                              record,
                              unit,
                              itemContinuationHandler,
                              nowEpochMs,
                              key,
                              attempt + 1),
                          retryDelayMs,
                          TimeUnit.MILLISECONDS);
                      return;
                    }
                    failParentAfterContinuationError(
                        new AwaitContinuationPlan.FailParent(record, failure, attempt));
                  }));
    } catch (RuntimeException failure) {
      permit.close();
      claims.releaseDispatch(key);
      throw failure;
    }
  }

  private Uni<Void> captureOutput(
      AwaitContinuationPlan.RecordItemOutput plan,
      long nowEpochMs) {
    return executionStateStore.getExecutionByKey(plan.interaction().tenantId(), plan.key().childExecutionKey())
        .onItem().transformToUni(existing -> {
          if (existing.isPresent() && existing.get().status() == ExecutionStatus.SUCCEEDED) {
            return recordItemContinuationSegment(plan, nowEpochMs)
                .chain(() -> releaseParentIfReady(plan.parent(), plan.unit(), plan.aggregateStepIndex(), nowEpochMs));
          }
          long ttl = plan.parent().ttlEpochS();
          ExecutionCreateCommand create = new ExecutionCreateCommand(
              plan.interaction().tenantId(),
              plan.key().childExecutionKey(),
              plan.parent().pipelineId(),
              plan.parent().contractVersion(),
              plan.parent().releaseVersion(),
              plan.continuationInput(),
              ExecutionResultShape.MATERIALIZED_MULTI,
              nowEpochMs,
              ttl);
          return executionStateStore.createOrGetExecution(create)
              .onItem().transformToUni(created -> handleCreatedChild(plan, created, nowEpochMs));
        });
  }

  private Uni<Void> handleCreatedChild(
      AwaitContinuationPlan.RecordItemOutput plan,
      CreateExecutionResult created,
      long nowEpochMs) {
    ExecutionRecord<Object, Object> child = created.record();
    if (created.duplicate() && child.status() == ExecutionStatus.SUCCEEDED) {
      return recordItemContinuationSegment(plan, nowEpochMs)
          .chain(() -> releaseParentIfReady(plan.parent(), plan.unit(), plan.aggregateStepIndex(), nowEpochMs));
    }
    String transitionKey = "await-item-continuation:" + plan.unit().unitId() + ":" + plan.interaction().itemIndex();
    return executionStateStore.markSucceeded(
            child.tenantId(),
            child.executionId(),
            child.version(),
            transitionKey,
            plan.segmentOutputs(),
            nowEpochMs)
        .onItem().transformToUni(updated -> handleChildSuccessUpdate(plan, updated, child, nowEpochMs));
  }

  private Uni<Void> handleChildSuccessUpdate(
      AwaitContinuationPlan.RecordItemOutput plan,
      Optional<ExecutionRecord<Object, Object>> updated,
      ExecutionRecord<Object, Object> child,
      long nowEpochMs) {
    if (updated.isPresent()) {
      return recordItemContinuationSegment(plan, nowEpochMs)
          .chain(() -> releaseParentWhenLocallyReady(
              plan.parent(),
              plan.unit(),
              plan.interaction().itemIndex(),
              plan.aggregateStepIndex(),
              nowEpochMs));
    }
    return executionStateStore.getExecutionByKey(plan.parent().tenantId(), child.executionKey())
        .onItem().transformToUni(current -> {
          if (current.isPresent() && current.get().status() == ExecutionStatus.SUCCEEDED) {
            return recordItemContinuationSegment(plan, nowEpochMs)
                .chain(() -> releaseParentIfReady(plan.parent(), plan.unit(), plan.aggregateStepIndex(), nowEpochMs));
          }
          return Uni.createFrom().failure(new IllegalStateException(
              "Await item continuation child success was not admitted and child is not already SUCCEEDED: "
                  + child.executionKey()));
        });
  }

  private Uni<Void> recordItemContinuationSegment(
      AwaitContinuationPlan.RecordItemOutput plan,
      long nowEpochMs) {
    return segmentBoundaryLedger.get().recordContinuationSegmentCreated(
        plan.parent(),
        plan.unit(),
        plan.key().segmentId(),
        plan.interaction().stepIndex() + 1,
        plan.aggregateStepIndex(),
        plan.continuationInput(),
        nowEpochMs);
  }

  private Uni<Void> releaseParentWhenLocallyReady(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      Integer itemIndex,
      int aggregateStepIndex,
      long nowEpochMs) {
    if (itemIndex == null || !claims.recordCompleted(parent, unit, itemIndex)) {
      return Uni.createFrom().voidItem();
    }
    return releaseParentIfReady(parent, unit, aggregateStepIndex, nowEpochMs);
  }

  private Uni<Void> releaseParent(
      ItemizedParentRelease release,
      long nowEpochMs) {
    return executionStateStore.markAwaitItemContinuationsCompleted(
            release.parent().tenantId(),
            release.parent().executionId(),
            release.unit().unitId(),
            release.aggregateStepIndex(),
            release.resumePayload(),
            nowEpochMs)
        .onItem().transformToUni(updated -> updated
            .map(released -> recordReleaseAndEnqueue(released, release, nowEpochMs))
            .orElseGet(() -> enqueueAlreadyReleasedParent(release, nowEpochMs)));
  }

  private Uni<Void> enqueueAlreadyReleasedParent(
      ItemizedParentRelease release,
      long nowEpochMs) {
    return executionStateStore.getExecution(release.parent().tenantId(), release.parent().executionId())
        .onItem().transformToUni(current -> {
          if (current.isEmpty() || !releasedForStep(current.get(), release.aggregateStepIndex())) {
            return Uni.createFrom().voidItem();
          }
          return recordReleaseAndEnqueue(current.get(), release, nowEpochMs);
        });
  }

  private Uni<Void> recordReleaseAndEnqueue(
      ExecutionRecord<Object, Object> released,
      ItemizedParentRelease release,
      long nowEpochMs) {
    return segmentBoundaryLedger.get().recordContinuationSegmentCreated(
            released,
            release.unit(),
            SegmentBoundaryLedger.segmentId(released),
            release.aggregateStepIndex(),
            -1,
            release.resumePayload(),
            nowEpochMs)
        .chain(() -> workDispatcher.enqueueNow(new ExecutionWorkItem(
            released.tenantId(),
            released.executionId())))
        .invoke(() -> {
          claims.clearDispatches(release.unit());
          claims.clearCompletions(released, release.unit());
          lifecycleRecorder.accept(new AwaitReplayLifecycleEvent(
              AwaitReplayLifecycleEvent.RESUME_RELEASED,
              released.executionId(),
              release.unit().unitId(),
              release.unit().stepId(),
              release.unit().stepIndex(),
              released.status().name(),
              null,
              null,
              null,
              null,
              release.unit().expectedItemCount(),
              release.unit().completedItemCount(),
              release.unit().dispatchComplete()));
          AwaitCompletionMetrics.recordResumeReleased(release.unit());
        })
        .replaceWithVoid();
  }

  private void failParentAfterContinuationError(AwaitContinuationPlan.FailParent plan) {
    AwaitInteractionRecord record = plan.interaction();
    long nowEpochMs = System.currentTimeMillis();
    executionStateStore.getExecution(record.tenantId(), record.executionId())
        .onItem().transformToUni(parent -> {
          if (parent.isEmpty() || parent.get().status().terminal()) {
            return Uni.createFrom().item(Optional.<ExecutionRecord<Object, Object>>empty());
          }
          ExecutionRecord<Object, Object> parentRecord = parent.get();
          return executionStateStore.markTerminalFailure(
              parentRecord.tenantId(),
              parentRecord.executionId(),
              parentRecord.version(),
              ExecutionStatus.FAILED,
              "await-item-continuation-failed:" + record.unitId() + ":" + record.itemIndex(),
              "AWAIT_ITEM_CONTINUATION_FAILED",
              "Await item continuation failed after " + plan.attempt() + " attempts: " + plan.failure().getMessage(),
              nowEpochMs);
        })
        .subscribe().with(
            ignored -> LOG.errorf(
                plan.failure(),
                "Failed processing await item continuation tenant=%s executionId=%s unitId=%s interactionId=%s itemIndex=%s after %d attempts; marked parent failed when still active",
                record.tenantId(),
                record.executionId(),
                record.unitId(),
                record.interactionId(),
                record.itemIndex(),
                plan.attempt()),
            persistenceFailure -> LOG.errorf(
                persistenceFailure,
                "Failed marking parent execution failed after await item continuation failure tenant=%s executionId=%s unitId=%s interactionId=%s itemIndex=%s",
                record.tenantId(),
                record.executionId(),
                record.unitId(),
                record.interactionId(),
                record.itemIndex()));
  }

  private static long retryDelayMs(int attempt) {
    int boundedAttempt = Math.max(1, Math.min(attempt, MAX_ATTEMPTS));
    return RETRY_BASE_MS << (boundedAttempt - 1);
  }

  private static boolean releasedForStep(
      ExecutionRecord<Object, Object> record,
      int stepIndex) {
    return record != null
        && record.status() == ExecutionStatus.QUEUED
        && record.currentStepIndex() == stepIndex;
  }
}
