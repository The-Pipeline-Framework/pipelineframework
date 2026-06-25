package org.pipelineframework;

import java.time.Duration;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitThrowableSupport;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneTransitionAdmission;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;

final class QueueAsyncExecutionFlow {

    private final PipelineOrchestratorConfig orchestratorConfig;
    private final ExecutionStateStore executionStateStore;
    private final WorkDispatcher workDispatcher;
    private final TransitionWorkerExecutor transitionWorkerExecutor;
    private final String queueWorkerId;
    private final Effects effects;

    QueueAsyncExecutionFlow(
        PipelineOrchestratorConfig orchestratorConfig,
        ExecutionStateStore executionStateStore,
        WorkDispatcher workDispatcher,
        TransitionWorkerExecutor transitionWorkerExecutor,
        String queueWorkerId,
        Effects effects) {
        this.orchestratorConfig = orchestratorConfig;
        this.executionStateStore = executionStateStore;
        this.workDispatcher = workDispatcher;
        this.transitionWorkerExecutor = transitionWorkerExecutor;
        this.queueWorkerId = queueWorkerId;
        this.effects = effects;
    }

    Uni<Void> processExecutionWorkItem(
        ExecutionWorkItem workItem,
        PipelineTransitionWorker worker,
        AwaitItemContinuationHandler itemContinuationHandler) {
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || workItem == null) {
            return Uni.createFrom().voidItem();
        }
        if (worker == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("PipelineTransitionWorker must not be null"));
        }
        Optional<TransitionWorkerExecutor.TransitionAdmission> admission = transitionWorkerExecutor.tryAdmit();
        if (admission.isEmpty()) {
            return workDispatcher.enqueueDelayed(workItem, effects.saturatedDelay());
        }
        TransitionWorkerExecutor.TransitionAdmission permit = admission.get();
        ControlPlaneTransitionAdmission tenantAdmission = effects.admitTransition(workItem);
        if (!tenantAdmission.decision().allowed()) {
            permit.close();
            if (ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED.equals(
                tenantAdmission.decision().errorCode())) {
                return workDispatcher.enqueueDelayed(workItem, effects.saturatedDelay());
            }
            effects.recordSkipped(workItem, tenantAdmission);
            return Uni.createFrom().voidItem();
        }
        long now = System.currentTimeMillis();
        return executionStateStore.claimLease(
                workItem.tenantId(),
                workItem.executionId(),
                queueWorkerId,
                now,
                orchestratorConfig.leaseMs())
            .onItem().transformToUni(claimed ->
                claimed
                    .map(record -> executeClaimed(record, worker, itemContinuationHandler))
                    .orElseGet(() -> Uni.createFrom().voidItem()))
            .onTermination().invoke(() -> {
                permit.close();
                tenantAdmission.permit().close();
            });
    }

    private Uni<Void> executeClaimed(
        ExecutionRecord<Object, Object> record,
        PipelineTransitionWorker worker,
        AwaitItemContinuationHandler itemContinuationHandler) {
        effects.recordClaimed(record);
        String transitionKey = transitionKey(record);
        return effects.prepareTransitionCommand(record, transitionKey)
            .onItem().transformToUni(command -> transitionWorkerExecutor.execute(worker, command))
            .onItem().transformToUni(result -> effects.commitTransition(
                record,
                transitionKey,
                result,
                itemContinuationHandler))
            .onFailure(AwaitThrowableSupport::containsAwaitSuspension).recoverWithUni(failure ->
                effects.suspensionSnapshot(failure)
                    .onItem().transformToUni(suspension -> effects.markWaitingExternal(
                        record,
                        transitionKey,
                        suspension,
                        itemContinuationHandler)))
            .onFailure().recoverWithUni(failure -> effects.handleExecutionFailure(record, transitionKey, failure));
    }

    private static String transitionKey(ExecutionRecord<Object, Object> record) {
        return record.executionId() + ":" + record.currentStepIndex() + ":" + record.attempt();
    }

    interface Effects {
        ControlPlaneTransitionAdmission admitTransition(ExecutionWorkItem workItem);

        Duration saturatedDelay();

        Uni<TransitionCommandEnvelope> prepareTransitionCommand(
            ExecutionRecord<Object, Object> record,
            String transitionKey);

        Uni<Void> commitTransition(
            ExecutionRecord<Object, Object> record,
            String transitionKey,
            TransitionResultEnvelope result,
            AwaitItemContinuationHandler itemContinuationHandler);

        Uni<TransitionAwaitSuspension> suspensionSnapshot(Throwable failure);

        Uni<Void> markWaitingExternal(
            ExecutionRecord<Object, Object> record,
            String transitionKey,
            TransitionAwaitSuspension suspension,
            AwaitItemContinuationHandler itemContinuationHandler);

        Uni<Void> handleExecutionFailure(
            ExecutionRecord<Object, Object> record,
            String transitionKey,
            Throwable failure);

        void recordClaimed(ExecutionRecord<Object, Object> record);

        void recordSkipped(ExecutionWorkItem workItem, ControlPlaneTransitionAdmission admission);
    }
}
