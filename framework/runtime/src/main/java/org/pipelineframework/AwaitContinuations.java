package org.pipelineframework;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCompletionMetrics;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

/**
 * Owns durable continuations that begin after an await boundary.
 */
class AwaitContinuations {

  private static final Logger LOG = Logger.getLogger(AwaitContinuations.class);
  private static final int AWAIT_ITEM_CONTINUATION_MAX_ATTEMPTS = 3;
  private static final long AWAIT_ITEM_CONTINUATION_RETRY_BASE_MS = 100L;
  private static final String ONE_TO_ONE_CARDINALITY = "ONE_TO_ONE";

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

  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final AwaitCoordinator awaitCoordinator;
  private final TransitionWorkerExecutor transitionWorkerExecutor;
  private final ScheduledExecutorService queueSweepExecutor;
  private final Supplier<Duration> saturatedDelay;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder;
  private final Set<String> itemContinuationDispatchClaims = ConcurrentHashMap.newKeySet();

  AwaitContinuations(
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitCoordinator awaitCoordinator,
      TransitionWorkerExecutor transitionWorkerExecutor,
      ScheduledExecutorService queueSweepExecutor,
      Supplier<Duration> saturatedDelay,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      Consumer<AwaitReplayLifecycleEvent> lifecycleRecorder) {
    this.executionStateStore = executionStateStore;
    this.workDispatcher = workDispatcher;
    this.awaitCoordinator = awaitCoordinator;
    this.transitionWorkerExecutor = transitionWorkerExecutor;
    this.queueSweepExecutor = queueSweepExecutor;
    this.saturatedDelay = saturatedDelay;
    this.segmentBoundaryLedger = segmentBoundaryLedger;
    this.lifecycleRecorder = lifecycleRecorder;
  }

  Uni<AwaitCompletionResult> afterRecordedCompletion(
      AwaitCompletionResult result,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    if (usesItemContinuations(result.record(), unit)) {
      return dispatchCompletedItemContinuationsIfReady(
              result.record(),
              unit,
              itemContinuationHandler,
              nowEpochMs)
          .replaceWith(result);
    }
    return unit.status() == AwaitUnitStatus.COMPLETED
        ? releaseScalarResume(result.record(), unit.unitId(), nowEpochMs).replaceWith(result)
        : Uni.createFrom().item(result);
  }

  Uni<Void> afterParentWaiting(
      ExecutionRecord<Object, Object> record,
      TransitionAwaitSuspension suspended,
      long nowEpochMs,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return awaitCoordinator.getUnit(record.tenantId(), suspended.unitId())
        .onItem().transformToUni(unit -> {
          if (unit == null || unit.status() != AwaitUnitStatus.COMPLETED) {
            return unit != null && usesItemContinuations(unit)
                ? dispatchCompletedItemContinuations(record, unit, itemContinuationHandler, nowEpochMs)
                : Uni.createFrom().voidItem();
          }
          if (usesItemContinuations(unit)) {
            return dispatchCompletedItemContinuations(record, unit, itemContinuationHandler, nowEpochMs)
                .onItem().transformToUni(ignored -> itemContinuationHandler.releaseAwaitParentIfReady(
                    record,
                    unit,
                    suspended.stepIndex() + 1,
                    nowEpochMs));
          }
          return releaseScalarResume(
              record.tenantId(),
              record.executionId(),
              unit.unitId(),
              unit.stepId(),
              suspended.stepIndex(),
              null,
              null,
              null,
              null,
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
    if (!usesItemContinuations(interaction, unit)) {
      return Uni.createFrom().voidItem();
    }
    if (interaction.itemIndex() == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("Itemized await continuation requires itemIndex"));
    }
    Optional<IllegalArgumentException> invalidIndex = invalidItemIndex(interaction, unit);
    if (invalidIndex.isPresent()) {
      return Uni.createFrom().failure(invalidIndex.get());
    }
    if (continuationInput == null) {
      return Uni.createFrom().failure(new IllegalArgumentException(
          "Itemized await continuation requires normalized continuation input"));
    }
    ExecutionInputSnapshot normalizedInput = copyInputSnapshot(continuationInput);
    return executionStateStore.getExecution(interaction.tenantId(), interaction.executionId())
        .onItem().transformToUni(parent -> {
          if (parent.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          ExecutionRecord<Object, Object> parentRecord = parent.get();
          String childKey = itemContinuationKey(parentRecord, unit, interaction.itemIndex());
          return executionStateStore.getExecutionByKey(interaction.tenantId(), childKey)
              .onItem().transformToUni(existing -> {
                if (existing.isPresent() && existing.get().status() == ExecutionStatus.SUCCEEDED) {
                  return recordItemContinuationSegment(
                          parentRecord,
                          unit,
                          interaction,
                          aggregateStepIndex,
                          normalizedInput,
                          nowEpochMs)
                      .chain(() -> releaseParentIfReady(parentRecord, unit, aggregateStepIndex, nowEpochMs));
                }
                long ttl = parentRecord.ttlEpochS();
                ExecutionCreateCommand create = new ExecutionCreateCommand(
                    interaction.tenantId(),
                    childKey,
                    parentRecord.pipelineId(),
                    parentRecord.contractVersion(),
                    parentRecord.releaseVersion(),
                    normalizedInput,
                    ExecutionResultShape.MATERIALIZED_MULTI,
                    nowEpochMs,
                    ttl);
                return executionStateStore.createOrGetExecution(create)
                    .onItem().transformToUni(created -> {
                      ExecutionRecord<Object, Object> child = created.record();
                      if (created.duplicate() && child.status() == ExecutionStatus.SUCCEEDED) {
                        return recordItemContinuationSegment(
                                parentRecord,
                                unit,
                                interaction,
                                aggregateStepIndex,
                                normalizedInput,
                                nowEpochMs)
                            .chain(() -> releaseParentIfReady(parentRecord, unit, aggregateStepIndex, nowEpochMs));
                      }
                      String transitionKey = "await-item-continuation:" + unit.unitId() + ":" + interaction.itemIndex();
                      return executionStateStore.markSucceeded(
                              child.tenantId(),
                              child.executionId(),
                              child.version(),
                              transitionKey,
                              List.copyOf(segmentOutputs == null ? List.of() : segmentOutputs),
                              nowEpochMs)
                          .onItem().transformToUni(ignored -> recordItemContinuationSegment(
                              parentRecord,
                              unit,
                              interaction,
                              aggregateStepIndex,
                              normalizedInput,
                              nowEpochMs))
                          .onItem().transformToUni(ignored ->
                              releaseParentIfReady(parentRecord, unit, aggregateStepIndex, nowEpochMs));
                    });
              });
        });
  }

  Uni<Void> releaseParentIfReady(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      long nowEpochMs) {
    if (parent == null || unit == null || !usesItemContinuations(unit)
        || !unit.dispatchComplete() || unit.expectedItemCount() == null) {
      return Uni.createFrom().voidItem();
    }
    List<Uni<Optional<ExecutionRecord<Object, Object>>>> lookups = new ArrayList<>();
    for (int index = 0; index < unit.expectedItemCount(); index++) {
      lookups.add(executionStateStore.getExecutionByKey(parent.tenantId(), itemContinuationKey(parent, unit, index)));
    }
    return Uni.join().all(lookups).andCollectFailures()
        .onItem().transformToUni(children -> {
          List<Object> orderedOutputs = new ArrayList<>();
          for (Object childObject : children) {
            @SuppressWarnings("unchecked")
            Optional<ExecutionRecord<Object, Object>> child = (Optional<ExecutionRecord<Object, Object>>) childObject;
            if (child.isEmpty() || child.get().status() != ExecutionStatus.SUCCEEDED) {
              return Uni.createFrom().voidItem();
            }
            Object payload = child.get().resultPayload();
            if (payload instanceof Iterable<?> iterable) {
              iterable.forEach(orderedOutputs::add);
            } else if (payload != null) {
              orderedOutputs.add(payload);
            }
          }
          Object resumePayload = new ExecutionInputSnapshot(ExecutionInputShape.MULTI, List.copyOf(orderedOutputs));
          return executionStateStore.markAwaitItemContinuationsCompleted(
                  parent.tenantId(),
                  parent.executionId(),
                  unit.unitId(),
                  aggregateStepIndex,
                  resumePayload,
                  nowEpochMs)
              .onItem().transformToUni(updated -> {
                if (updated.isEmpty()) {
                  return enqueueAlreadyReleasedParent(
                      parent.tenantId(),
                      parent.executionId(),
                      unit,
                      aggregateStepIndex,
                      resumePayload,
                      nowEpochMs);
                }
                return recordItemizedParentReleaseAndEnqueue(
                    updated.get(),
                    unit,
                    aggregateStepIndex,
                    resumePayload,
                    nowEpochMs);
              });
        });
  }

  private Uni<Void> recordItemContinuationSegment(
      ExecutionRecord<Object, Object> parentRecord,
      AwaitUnitRecord unit,
      AwaitInteractionRecord interaction,
      int aggregateStepIndex,
      ExecutionInputSnapshot normalizedInput,
      long nowEpochMs) {
    return segmentBoundaryLedger.get().recordContinuationSegmentCreated(
        parentRecord,
        unit,
        SegmentBoundaryLedger.itemContinuationSegmentId(
            parentRecord.executionId(),
            unit.unitId(),
            interaction.itemIndex()),
        interaction.stepIndex() + 1,
        aggregateStepIndex,
        normalizedInput,
        nowEpochMs);
  }

  private Uni<Void> enqueueAlreadyReleasedParent(
      String tenantId,
      String executionId,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      Object resumePayload,
      long nowEpochMs) {
    return executionStateStore.getExecution(tenantId, executionId)
        .onItem().transformToUni(current -> {
          if (current.isEmpty() || !releasedForStep(current.get(), aggregateStepIndex)) {
            return Uni.createFrom().voidItem();
          }
          return recordItemizedParentReleaseAndEnqueue(
              current.get(),
              unit,
              aggregateStepIndex,
              resumePayload,
              nowEpochMs);
        });
  }

  private Uni<Void> recordItemizedParentReleaseAndEnqueue(
      ExecutionRecord<Object, Object> released,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      Object resumePayload,
      long nowEpochMs) {
    return segmentBoundaryLedger.get().recordContinuationSegmentCreated(
            released,
            unit,
            SegmentBoundaryLedger.segmentId(released),
            aggregateStepIndex,
            -1,
            resumePayload,
            nowEpochMs)
        .chain(() -> workDispatcher.enqueueNow(new ExecutionWorkItem(
            released.tenantId(),
            released.executionId())))
        .invoke(() -> {
          clearItemContinuationDispatchClaims(unit);
          lifecycleRecorder.accept(new AwaitReplayLifecycleEvent(
              AwaitReplayLifecycleEvent.RESUME_RELEASED,
              released.executionId(),
              unit.unitId(),
              unit.stepId(),
              unit.stepIndex(),
              released.status().name(),
              null,
              null,
              null,
              null,
              unit.expectedItemCount(),
              unit.completedItemCount(),
              unit.dispatchComplete()));
          AwaitCompletionMetrics.recordResumeReleased(unit);
        })
        .replaceWithVoid();
  }

  private Uni<Boolean> itemContinuationReady(
      AwaitInteractionRecord record,
      AwaitUnitRecord unit) {
    if (record == null || unit == null || !unit.dispatchComplete()) {
      return Uni.createFrom().item(false);
    }
    Uni<Optional<ExecutionRecord<Object, Object>>> parentLookup =
        executionStateStore.getExecution(record.tenantId(), record.executionId());
    if (parentLookup == null) {
      return Uni.createFrom().item(false);
    }
    return parentLookup.onItem().transform(parent -> itemContinuationReady(parent, unit));
  }

  private static boolean itemContinuationReady(
      Optional<ExecutionRecord<Object, Object>> parent,
      AwaitUnitRecord unit) {
    return parent.isPresent()
        && parent.get().status() == ExecutionStatus.WAITING_EXTERNAL
        && unit.unitId().equals(parent.get().awaitUnitId());
  }

  private Uni<Void> dispatchCompletedItemContinuationsIfReady(
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
    return parentLookup.onItem().transformToUni(parent -> {
      if (!itemContinuationReady(parent, unit)) {
        AwaitCompletionMetrics.recordEarlyCompletionHeld(record, unit);
        return Uni.createFrom().voidItem();
      }
      return dispatchCompletedItemContinuations(parent.get(), unit, itemContinuationHandler, nowEpochMs);
    });
  }

  private Uni<Void> dispatchCompletedItemContinuations(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs) {
    if (parent == null || unit == null || !usesItemContinuations(unit) || !unit.dispatchComplete()
        || itemContinuationHandler == NOOP_ITEM_CONTINUATION_HANDLER) {
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
    if (itemContinuationHandler == NOOP_ITEM_CONTINUATION_HANDLER) {
      return;
    }
    Optional<IllegalArgumentException> invalidIndex = invalidItemIndex(record, unit);
    if (invalidIndex.isPresent()) {
      throw invalidIndex.get();
    }
    String claimKey = itemContinuationDispatchClaimKey(record);
    if (!itemContinuationDispatchClaims.add(claimKey)) {
      return;
    }
    dispatchItemContinuationAttempt(record, unit, itemContinuationHandler, nowEpochMs, claimKey, 1);
  }

  private void dispatchItemContinuationAttempt(
      AwaitInteractionRecord record,
      AwaitUnitRecord unit,
      AwaitItemContinuationHandler itemContinuationHandler,
      long nowEpochMs,
      String claimKey,
      int attempt) {
    Optional<TransitionWorkerExecutor.TransitionAdmission> admission = transitionWorkerExecutor.tryAdmit();
    if (admission.isEmpty()) {
      queueSweepExecutor.schedule(
          () -> dispatchItemContinuationAttempt(record, unit, itemContinuationHandler, nowEpochMs, claimKey, attempt),
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
                if (!itemContinuationReady(parent, unit)) {
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
                      itemContinuationDispatchClaims.remove(claimKey);
                    }
                  },
                  failure -> {
                    permit.close();
                    if (attempt < AWAIT_ITEM_CONTINUATION_MAX_ATTEMPTS) {
                      long retryDelayMs = itemContinuationRetryDelayMs(attempt);
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
                              claimKey,
                              attempt + 1),
                          retryDelayMs,
                          TimeUnit.MILLISECONDS);
                      return;
                    }
                    failParentAfterContinuationError(record, failure, attempt);
                  }));
    } catch (RuntimeException failure) {
      permit.close();
      itemContinuationDispatchClaims.remove(claimKey);
      throw failure;
    }
  }

  private static long itemContinuationRetryDelayMs(int attempt) {
    int boundedAttempt = Math.max(1, Math.min(attempt, AWAIT_ITEM_CONTINUATION_MAX_ATTEMPTS));
    return AWAIT_ITEM_CONTINUATION_RETRY_BASE_MS << (boundedAttempt - 1);
  }

  private void failParentAfterContinuationError(
      AwaitInteractionRecord record,
      Throwable failure,
      int attempt) {
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
              "Await item continuation failed after " + attempt + " attempts: " + failure.getMessage(),
              nowEpochMs);
        })
        .subscribe().with(
            ignored -> LOG.errorf(
                failure,
                "Failed processing await item continuation tenant=%s executionId=%s unitId=%s interactionId=%s itemIndex=%s after %d attempts; marked parent failed when still active",
                record.tenantId(),
                record.executionId(),
                record.unitId(),
                record.interactionId(),
                record.itemIndex(),
                attempt),
              persistenceFailure -> LOG.errorf(
                  persistenceFailure,
                  "Failed marking parent execution failed after await item continuation failure tenant=%s executionId=%s unitId=%s interactionId=%s itemIndex=%s",
                  record.tenantId(),
                  record.executionId(),
                  record.unitId(),
                  record.interactionId(),
                  record.itemIndex()));
  }

  private Optional<IllegalArgumentException> invalidItemIndex(
      AwaitInteractionRecord interaction,
      AwaitUnitRecord unit) {
    if (interaction == null || interaction.itemIndex() == null) {
      return Optional.of(new IllegalArgumentException("Itemized await continuation requires itemIndex"));
    }
    int itemIndex = interaction.itemIndex();
    if (itemIndex < 0) {
      return Optional.of(new IllegalArgumentException(
          "Itemized await continuation itemIndex must be non-negative: " + itemIndex));
    }
    Integer expectedItemCount = unit == null ? null : unit.expectedItemCount();
    if (expectedItemCount != null && itemIndex >= expectedItemCount) {
      return Optional.of(new IllegalArgumentException(
          "Itemized await continuation itemIndex " + itemIndex
              + " is outside expected item count " + expectedItemCount));
    }
    return Optional.empty();
  }

  private String itemContinuationDispatchClaimKey(AwaitInteractionRecord record) {
    return record.tenantId() + "::"
        + record.executionId() + "::"
        + record.unitId() + "::"
        + record.itemIndex();
  }

  private void clearItemContinuationDispatchClaims(AwaitUnitRecord unit) {
    if (unit == null) {
      return;
    }
    String prefix = unit.tenantId() + "::"
        + unit.executionId() + "::"
        + unit.unitId() + "::";
    itemContinuationDispatchClaims.removeIf(key -> key.startsWith(prefix));
  }

  private Uni<Void> releaseScalarResume(
      AwaitInteractionRecord record,
      String awaitUnitId,
      long nowEpochMs) {
    return releaseScalarResume(
        record.tenantId(),
        record.executionId(),
        awaitUnitId,
        record.stepId(),
        record.stepIndex(),
        record.interactionId(),
        record.correlationId(),
        record.transportType(),
        record.itemIndex(),
        nowEpochMs);
  }

  private Uni<Void> releaseScalarResume(
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
        .onItem().transformToUni(updated -> {
          if (updated.isPresent()) {
            return recordScalarResumeAndEnqueue(
                updated.get(),
                awaitUnitId,
                stepId,
                stepIndex,
                interactionId,
                correlationId,
                transportType,
                itemIndex,
                nowEpochMs);
          }
          LOG.debugf(
              "Await resume release for execution %s awaitUnitId=%s at stepIndex=%d produced no state update; treating as idempotent no-op",
              executionId,
              awaitUnitId,
              stepIndex);
          return enqueueAlreadyReleasedScalarResume(
              tenantId,
              executionId,
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

  private Uni<Void> enqueueAlreadyReleasedScalarResume(
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
    return executionStateStore.getExecution(tenantId, executionId)
        .onItem().transformToUni(current -> {
          if (current.isEmpty() || !releasedForStep(current.get(), stepIndex + 1)) {
            return Uni.createFrom().voidItem();
          }
          return recordScalarResumeAndEnqueue(
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

  private Uni<Void> recordScalarResumeAndEnqueue(
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

  private static boolean usesItemContinuations(
      AwaitInteractionRecord record,
      AwaitUnitRecord unit) {
    return record != null
        && record.itemInteraction()
        && usesItemContinuations(unit);
  }

  private static boolean usesItemContinuations(AwaitUnitRecord unit) {
    return unit != null
        && unit.primaryInteractionId() == null
        && ONE_TO_ONE_CARDINALITY.equalsIgnoreCase(unit.cardinality());
  }

  private static String itemContinuationKey(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int itemIndex) {
    return parent.executionKey() + ":await-item:" + unit.unitId() + ":" + itemIndex;
  }

  private static ExecutionInputSnapshot copyInputSnapshot(ExecutionInputSnapshot snapshot) {
    Object payload = snapshot.payload();
    if (payload instanceof List<?> list) {
      payload = List.copyOf(list);
    }
    return new ExecutionInputSnapshot(snapshot.shape(), payload);
  }
}
