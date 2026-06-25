package org.pipelineframework;

import java.util.Objects;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;

final class TransitionCommitFlow {

    private final AwaitCoordinator awaitCoordinator;
    private final ExecutionStateStore executionStateStore;
    private final WorkDispatcher workDispatcher;
    private final DeadLetterPublisher deadLetterPublisher;
    private final ExecutionFailureHandler executionFailureHandler;
    private final TerminalPublicationBarrier terminalPublicationBarrier;
    private final Effects effects;

    TransitionCommitFlow(
        AwaitCoordinator awaitCoordinator,
        ExecutionStateStore executionStateStore,
        WorkDispatcher workDispatcher,
        DeadLetterPublisher deadLetterPublisher,
        ExecutionFailureHandler executionFailureHandler,
        TerminalPublicationBarrier terminalPublicationBarrier,
        Effects effects) {
        this.awaitCoordinator = awaitCoordinator;
        this.executionStateStore = executionStateStore;
        this.workDispatcher = workDispatcher;
        this.deadLetterPublisher = deadLetterPublisher;
        this.executionFailureHandler = executionFailureHandler;
        this.terminalPublicationBarrier = Objects.requireNonNull(
            terminalPublicationBarrier,
            "terminalPublicationBarrier must not be null");
        this.effects = effects;
    }

    Uni<Void> commit(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionResultEnvelope result,
        AwaitItemContinuationHandler itemContinuationHandler) {
        return interpretPlan(
            TransitionCommitPlan.from(record, transitionKey, result, System.currentTimeMillis()),
            itemContinuationHandler);
    }

    Uni<Void> markWaitingExternal(
        QueueAsyncCommand.TransitionSuspended command,
        AwaitItemContinuationHandler itemContinuationHandler) {
        return markWaitingExternal(
            new TransitionCommitPlan.MarkWaitingExternal(
                command.record(),
                command.transitionKey(),
                command.suspension(),
                System.currentTimeMillis()),
            itemContinuationHandler);
    }

    private Uni<Void> interpretPlan(
        TransitionCommitPlan plan,
        AwaitItemContinuationHandler itemContinuationHandler) {
        return switch (plan) {
            case TransitionCommitPlan.MarkSucceeded succeeded -> markSucceeded(succeeded);
            case TransitionCommitPlan.MarkWaitingExternal waiting -> markWaitingExternal(waiting, itemContinuationHandler);
            case TransitionCommitPlan.HandleFailure failed -> handleFailure(
                failed.record(),
                failed.transitionKey(),
                failed.failure());
        };
    }

    private Uni<Void> markWaitingExternal(
        TransitionCommitPlan.MarkWaitingExternal plan,
        AwaitItemContinuationHandler itemContinuationHandler) {
        ExecutionRecord<Object, Object> record = plan.record();
        TransitionAwaitSuspension suspended = plan.suspension();
        return awaitCoordinator.importSuspension(suspended)
            .onItem().transformToUni(ignored -> {
                return executionStateStore.markWaitingExternal(
                    record.tenantId(),
                    record.executionId(),
                    record.version(),
                    plan.transitionKey(),
                    suspended.unitId(),
                    suspended.stepIndex(),
                    plan.nowEpochMs())
                    .onItem().transformToUni(updated -> {
                        if (updated.isPresent()) {
                            effects.recordExecutionWaiting(new AwaitReplayLifecycleEvent(
                                AwaitReplayLifecycleEvent.EXECUTION_WAITING,
                                record.executionId(),
                                suspended.unitId(),
                                null,
                                suspended.stepIndex(),
                                ExecutionStatus.WAITING_EXTERNAL.name(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null));
                            return effects.releaseAlreadyCompletedAwaitUnit(
                                updated.get(),
                                suspended,
                                plan.nowEpochMs(),
                                itemContinuationHandler);
                        }
                        return Uni.createFrom().failure(new IllegalStateException(
                            "Failed to persist WAITING_EXTERNAL state for execution "
                                + record.executionId()
                                + " at step "
                                + suspended.stepIndex()
                                + " (expectedVersion="
                                + record.version()
                                + ", awaitUnitId="
                                + suspended.unitId()
                                + ")"));
                    });
            });
    }

    private Uni<Void> markSucceeded(TransitionCommitPlan.MarkSucceeded plan) {
        return terminalPublicationBarrier.publishBeforeSuccess(plan.record(), plan.result())
            .replaceWith(plan.outputItems())
            .onItem().transformToUni(payload -> executionStateStore.markSucceeded(
                plan.record().tenantId(),
                plan.record().executionId(),
                plan.record().version(),
                plan.transitionKey(),
                payload,
                plan.nowEpochMs()))
            .replaceWithVoid();
    }

    private Uni<Void> handleFailure(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        Throwable failure) {
        return executionFailureHandler.handleExecutionFailure(
            record,
            transitionKey,
            failure,
            executionStateStore,
            workDispatcher,
            deadLetterPublisher);
    }

    interface Effects {
        Uni<Void> releaseAlreadyCompletedAwaitUnit(
            ExecutionRecord<Object, Object> record,
            TransitionAwaitSuspension suspended,
            long nowEpochMs,
            AwaitItemContinuationHandler itemContinuationHandler);

        void recordExecutionWaiting(AwaitReplayLifecycleEvent lifecycleEvent);
    }
}
