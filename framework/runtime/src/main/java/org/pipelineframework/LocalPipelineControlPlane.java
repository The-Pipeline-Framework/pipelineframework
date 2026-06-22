package org.pipelineframework;

import java.util.List;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.ExecutionRedriveResult;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Local in-process control-plane facade backed by the queue async coordinator.
 */
@ApplicationScoped
public class LocalPipelineControlPlane implements PipelineControlPlane {

    @Inject
    QueueAsyncCoordinator queueAsyncCoordinator;

    @PostConstruct
    void initialize() {
        initializeQueueMode();
    }

    @Override
    public void initializeQueueMode() {
        queueAsyncCoordinator.initializeQueueMode();
    }

    @Override
    public Uni<RunAsyncAcceptedDto> executePipelineAsync(
        Object input,
        String tenantId,
        String idempotencyKey,
        boolean outputStreaming) {
        return queueAsyncCoordinator.executePipelineAsync(input, tenantId, idempotencyKey, outputStreaming);
    }

    @Override
    public Uni<RunAsyncAcceptedDto> executePipelineAsync(
        Object input,
        String tenantId,
        String idempotencyKey,
        boolean outputStreaming,
        String pipelineId,
        String contractVersion,
        String releaseVersion) {
        return queueAsyncCoordinator.executePipelineAsync(
            input,
            tenantId,
            idempotencyKey,
            outputStreaming,
            pipelineId,
            contractVersion,
            releaseVersion);
    }

    @Override
    public Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
        return queueAsyncCoordinator.getExecutionStatus(tenantId, executionId);
    }

    @Override
    public <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
        return queueAsyncCoordinator.getExecutionResult(tenantId, executionId, outputType, outputStreaming);
    }

    @Override
    public Uni<Object> getExecutionResultPayload(String tenantId, String executionId) {
        return queueAsyncCoordinator.getExecutionResultPayload(tenantId, executionId);
    }

    @Override
    public Uni<ExecutionRedriveResult> redriveExecution(
        String tenantId,
        String executionId,
        Long expectedVersion,
        boolean allowFailed,
        String reason) {
        return queueAsyncCoordinator.redriveExecution(tenantId, executionId, expectedVersion, allowFailed, reason);
    }

    @Override
    public Uni<AwaitCompletionResult> completeAwait(AwaitCompletionCommand command) {
        return queueAsyncCoordinator.completeAwait(command);
    }

    @Override
    public Uni<AwaitCompletionResult> completeAwait(
        AwaitCompletionCommand command,
        AwaitItemContinuationHandler itemContinuationHandler) {
        return queueAsyncCoordinator.completeAwait(command, itemContinuationHandler);
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> queryPendingAwaitInteractions(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return queueAsyncCoordinator.queryPendingAwaitInteractions(tenantId, assignee, group, stepId, limit);
    }

    @Override
    public Uni<Void> processExecutionWorkItem(ExecutionWorkItem workItem, PipelineTransitionWorker worker) {
        return queueAsyncCoordinator.processExecutionWorkItem(workItem, worker);
    }

    @Override
    public Uni<Void> processExecutionWorkItem(
        ExecutionWorkItem workItem,
        PipelineTransitionWorker worker,
        AwaitItemContinuationHandler itemContinuationHandler) {
        return queueAsyncCoordinator.processExecutionWorkItem(workItem, worker, itemContinuationHandler);
    }
}
