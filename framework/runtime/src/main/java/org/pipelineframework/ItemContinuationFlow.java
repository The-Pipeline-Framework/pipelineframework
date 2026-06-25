package org.pipelineframework;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCompletionMetrics;
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
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

final class ItemContinuationFlow {

    private static final Logger LOG = Logger.getLogger(ItemContinuationFlow.class);
    private static final int MAX_ATTEMPTS = 3;
    private static final long RETRY_BASE_MS = 100L;
    private static final String ONE_TO_ONE_CARDINALITY = "ONE_TO_ONE";

    private final AwaitCoordinator awaitCoordinator;
    private final ExecutionStateStore executionStateStore;
    private final WorkDispatcher workDispatcher;
    private final TransitionWorkerExecutor transitionWorkerExecutor;
    private final Scheduler scheduler;
    private final Executor continuationExecutor;
    private final Supplier<Duration> saturatedDelay;
    private final AwaitItemContinuationHandler noopHandler;
    private final NonItemAwaitReleaser nonItemAwaitReleaser;
    private final Effects effects;

    ItemContinuationFlow(
        AwaitCoordinator awaitCoordinator,
        ExecutionStateStore executionStateStore,
        WorkDispatcher workDispatcher,
        TransitionWorkerExecutor transitionWorkerExecutor,
        Scheduler scheduler,
        Executor continuationExecutor,
        Supplier<Duration> saturatedDelay,
        AwaitItemContinuationHandler noopHandler,
        NonItemAwaitReleaser nonItemAwaitReleaser,
        Effects effects) {
        this.awaitCoordinator = awaitCoordinator;
        this.executionStateStore = executionStateStore;
        this.workDispatcher = workDispatcher;
        this.transitionWorkerExecutor = transitionWorkerExecutor;
        this.scheduler = scheduler;
        this.continuationExecutor = continuationExecutor;
        this.saturatedDelay = saturatedDelay;
        this.noopHandler = noopHandler;
        this.nonItemAwaitReleaser = nonItemAwaitReleaser;
        this.effects = effects;
    }

    Uni<Boolean> itemContinuationReady(AwaitInteractionRecord record, AwaitUnitRecord unit) {
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

    void dispatchItemContinuation(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        AwaitItemContinuationHandler itemContinuationHandler,
        long nowEpochMs) {
        if (itemContinuationHandler == null || itemContinuationHandler == noopHandler) {
            return;
        }
        dispatchItemContinuationAttempt(record, unit, itemContinuationHandler, nowEpochMs, 1);
    }

    Uni<Void> releaseAlreadyCompletedAwaitUnit(
        ExecutionRecord<Object, Object> record,
        TransitionAwaitSuspension suspended,
        long nowEpochMs,
        AwaitItemContinuationHandler itemContinuationHandler) {
        return awaitCoordinator.getUnit(record.tenantId(), suspended.unitId())
            .onItem().transformToUni(unit -> {
                if (unit == null || unit.status() != AwaitUnitStatus.COMPLETED) {
                    return unit != null && usesItemContinuations(unit)
                        ? dispatchCompletedAwaitItemContinuations(record, unit, itemContinuationHandler, nowEpochMs)
                        : Uni.createFrom().voidItem();
                }
                if (usesItemContinuations(unit)) {
                    return dispatchCompletedAwaitItemContinuations(record, unit, itemContinuationHandler, nowEpochMs)
                        .onItem().transformToUni(ignored -> itemContinuationHandler.releaseAwaitParentIfReady(
                            record,
                            unit,
                            suspended.stepIndex() + 1,
                            nowEpochMs));
                }
                return nonItemAwaitReleaser.release(record, unit, suspended, nowEpochMs);
            });
    }

    Uni<Void> recordAwaitItemContinuation(
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
                String childKey = awaitItemContinuationKey(parentRecord, unit, interaction.itemIndex());
                return executionStateStore.getExecutionByKey(interaction.tenantId(), childKey)
                    .onItem().transformToUni(existing -> {
                        if (existing.isPresent() && existing.get().status() == ExecutionStatus.SUCCEEDED) {
                            return releaseItemizedAwaitParentIfReady(parentRecord, unit, aggregateStepIndex, nowEpochMs);
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
                                    return releaseItemizedAwaitParentIfReady(parentRecord, unit, aggregateStepIndex, nowEpochMs);
                                }
                                String transitionKey = "await-item-continuation:" + unit.unitId() + ":" + interaction.itemIndex();
                                return executionStateStore.markSucceeded(
                                        child.tenantId(),
                                        child.executionId(),
                                        child.version(),
                                        transitionKey,
                                        List.copyOf(segmentOutputs == null ? List.of() : segmentOutputs),
                                        nowEpochMs)
                                    .onItem().transformToUni(ignored ->
                                        releaseItemizedAwaitParentIfReady(parentRecord, unit, aggregateStepIndex, nowEpochMs));
                            });
                    });
            });
    }

    Uni<Void> releaseItemizedAwaitParentIfReady(
        ExecutionRecord<Object, Object> parent,
        AwaitUnitRecord unit,
        int aggregateStepIndex,
        long nowEpochMs) {
        return releaseItemizedAwaitParentIfReady(
            new QueueAsyncCommand.ReleaseItemizedParent(parent, unit, aggregateStepIndex, nowEpochMs));
    }

    private Uni<Void> releaseItemizedAwaitParentIfReady(QueueAsyncCommand.ReleaseItemizedParent command) {
        ExecutionRecord<Object, Object> parent = command.parent();
        AwaitUnitRecord unit = command.unit();
        if (parent == null || unit == null || !usesItemContinuations(unit)
            || !unit.dispatchComplete() || unit.expectedItemCount() == null) {
            return Uni.createFrom().voidItem();
        }
        List<Uni<Optional<ExecutionRecord<Object, Object>>>> lookups = new ArrayList<>();
        for (int index = 0; index < unit.expectedItemCount(); index++) {
            lookups.add(executionStateStore.getExecutionByKey(
                parent.tenantId(),
                awaitItemContinuationKey(parent, unit, index)));
        }
        return Uni.join().all(lookups).andCollectFailures()
            .onItem().transformToUni(children -> {
                List<Object> orderedOutputs = new ArrayList<>();
                for (Object childObject : children) {
                    @SuppressWarnings("unchecked")
                    Optional<ExecutionRecord<Object, Object>> child =
                        (Optional<ExecutionRecord<Object, Object>>) childObject;
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
                        command.aggregateStepIndex(),
                        resumePayload,
                        command.nowEpochMs())
                    .onItem().transformToUni(updated -> {
                        if (updated.isEmpty()) {
                            return Uni.createFrom().voidItem();
                        }
                        AwaitReplayLifecycleEvent lifecycleEvent = new AwaitReplayLifecycleEvent(
                            AwaitReplayLifecycleEvent.RESUME_RELEASED,
                            parent.executionId(),
                            unit.unitId(),
                            unit.stepId(),
                            unit.stepIndex(),
                            updated.get().status().name(),
                            null,
                            null,
                            null,
                            null,
                            unit.expectedItemCount(),
                            unit.completedItemCount(),
                            unit.dispatchComplete());
                        effects.recordAwaitLifecycle(lifecycleEvent);
                        AwaitCompletionMetrics.recordResumeReleased(unit);
                        return workDispatcher.enqueueNow(new ExecutionWorkItem(
                                updated.get().tenantId(),
                                updated.get().executionId()))
                            .replaceWithVoid();
                    });
            });
    }

    private Uni<Void> dispatchCompletedAwaitItemContinuations(
        ExecutionRecord<Object, Object> parent,
        AwaitUnitRecord unit,
        AwaitItemContinuationHandler itemContinuationHandler,
        long nowEpochMs) {
        if (parent == null || unit == null || !usesItemContinuations(unit) || !unit.dispatchComplete()
            || itemContinuationHandler == null || itemContinuationHandler == noopHandler) {
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

    private void dispatchItemContinuationAttempt(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        AwaitItemContinuationHandler itemContinuationHandler,
        long nowEpochMs,
        int attempt) {
        Optional<TransitionWorkerExecutor.TransitionAdmission> admission = transitionWorkerExecutor.tryAdmit();
        if (admission.isEmpty()) {
            if (attempt >= MAX_ATTEMPTS) {
                markAwaitItemContinuationFailed(
                    record,
                    new IllegalStateException("Transition worker admission remained saturated"),
                    attempt);
                return;
            }
            scheduler.schedule(
                () -> dispatchItemContinuationAttempt(record, unit, itemContinuationHandler, nowEpochMs, attempt + 1),
                saturatedDelay.get().toMillis());
            return;
        }
        TransitionWorkerExecutor.TransitionAdmission permit = admission.get();
        try {
            continuationExecutor.execute(() -> runAdmittedContinuation(
                permit,
                record,
                unit,
                itemContinuationHandler,
                nowEpochMs,
                attempt));
        } catch (Throwable failure) {
            permit.close();
            handleContinuationFailure(record, unit, itemContinuationHandler, nowEpochMs, attempt, failure);
        }
    }

    private void runAdmittedContinuation(
        TransitionWorkerExecutor.TransitionAdmission permit,
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        AwaitItemContinuationHandler itemContinuationHandler,
        long nowEpochMs,
        int attempt) {
        try {
            Uni<Void> continuation = executionStateStore
                .getExecution(record.tenantId(), record.executionId())
                .onItem().transformToUni(parent -> {
                    if (!itemContinuationReady(parent, unit)) {
                        return Uni.createFrom().voidItem();
                    }
                    return itemContinuationHandler.continueAwaitItem(
                        record,
                        unit,
                        record.stepIndex() + 1,
                        parent,
                        nowEpochMs);
                });
            continuation.subscribe().with(
                ignored -> permit.close(),
                failure -> {
                    permit.close();
                    handleContinuationFailure(record, unit, itemContinuationHandler, nowEpochMs, attempt, failure);
                });
        } catch (Throwable failure) {
            permit.close();
            handleContinuationFailure(record, unit, itemContinuationHandler, nowEpochMs, attempt, failure);
        }
    }

    private void handleContinuationFailure(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        AwaitItemContinuationHandler itemContinuationHandler,
        long nowEpochMs,
        int attempt,
        Throwable failure) {
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
            scheduler.schedule(
                () -> dispatchItemContinuationAttempt(
                    record,
                    unit,
                    itemContinuationHandler,
                    nowEpochMs,
                    attempt + 1),
                retryDelayMs);
            return;
        }
        markAwaitItemContinuationFailed(record, failure, attempt);
    }

    private void markAwaitItemContinuationFailed(
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

    static boolean usesItemContinuations(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        return record != null
            && record.itemInteraction()
            && usesItemContinuations(unit);
    }

    static boolean usesItemContinuations(AwaitUnitRecord unit) {
        return unit != null
            && unit.primaryInteractionId() == null
            && ONE_TO_ONE_CARDINALITY.equalsIgnoreCase(unit.cardinality());
    }

    private static boolean itemContinuationReady(Optional<ExecutionRecord<Object, Object>> parent, AwaitUnitRecord unit) {
        return parent.isPresent()
            && parent.get().status() == ExecutionStatus.WAITING_EXTERNAL
            && unit.unitId().equals(parent.get().awaitUnitId());
    }

    private static ExecutionInputSnapshot copyInputSnapshot(ExecutionInputSnapshot snapshot) {
        Object payload = snapshot.payload();
        if (payload instanceof List<?> list) {
            payload = List.copyOf(list);
        }
        return new ExecutionInputSnapshot(snapshot.shape(), payload);
    }

    private static String awaitItemContinuationKey(
        ExecutionRecord<Object, Object> parent,
        AwaitUnitRecord unit,
        int itemIndex) {
        return parent.executionKey() + ":await-item:" + unit.unitId() + ":" + itemIndex;
    }

    private static long retryDelayMs(int attempt) {
        int boundedAttempt = Math.max(1, Math.min(attempt, MAX_ATTEMPTS));
        return RETRY_BASE_MS << (boundedAttempt - 1);
    }

    interface Scheduler {
        void schedule(Runnable task, long delayMs);
    }

    interface NonItemAwaitReleaser {
        Uni<Void> release(
            ExecutionRecord<Object, Object> record,
            AwaitUnitRecord unit,
            TransitionAwaitSuspension suspended,
            long nowEpochMs);
    }

    interface Effects {
        void recordAwaitLifecycle(AwaitReplayLifecycleEvent lifecycleEvent);
    }
}
