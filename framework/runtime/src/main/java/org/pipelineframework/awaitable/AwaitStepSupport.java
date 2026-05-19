package org.pipelineframework.awaitable;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/**
 * Runtime bridge used by generated await step beans.
 */
@ApplicationScoped
public class AwaitStepSupport {

    @Inject
    AwaitCoordinator awaitCoordinator;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    /**
     * Creates/dispatches an await interaction and suspends queue-async execution.
     */
    @SuppressWarnings("unchecked")
    public <I, O> Uni<O> awaitOneToOne(AwaitStepDescriptor descriptor, I input) {
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Await steps require pipeline.orchestrator.mode=QUEUE_ASYNC."));
        }
        AwaitExecutionContext context = AwaitExecutionContextHolder.get();
        if (context == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Await step executed without queue-async execution context."));
        }
        int stepIndex = context.currentStepIndex();
        return awaitCoordinator.createOrGet(
                descriptor,
                context.tenantId(),
                context.executionId(),
                stepIndex,
                context.executionId() + ":" + stepIndex,
                input,
                null,
                null)
            .onItem().transformToUni(created -> {
                AwaitInteractionRecord record = created.record();
                Uni<AwaitInteractionRecord> dispatched = record.status() == AwaitInteractionStatus.WAITING
                    ? awaitCoordinator.dispatch(descriptor, record)
                    : Uni.createFrom().item(record);
                return dispatched.onItem().transformToUni(updated ->
                    Uni.createFrom().failure(new AwaitSuspendedException(
                        context.tenantId(),
                        context.executionId(),
                        updated.interactionId(),
                        stepIndex)));
            });
    }
}
