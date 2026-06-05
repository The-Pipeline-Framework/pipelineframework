package org.pipelineframework.orchestrator;

import java.util.List;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Internal facade for queue-async orchestration ownership.
 */
public interface PipelineControlPlane {

    /**
     * Initializes the local queue-async control plane if queue mode is enabled.
     */
    void initializeQueueMode();

    Uni<RunAsyncAcceptedDto> executePipelineAsync(
        Object input,
        String tenantId,
        String idempotencyKey,
        boolean outputStreaming);

    Uni<RunAsyncAcceptedDto> executePipelineAsync(
        Object input,
        String tenantId,
        String idempotencyKey,
        boolean outputStreaming,
        String pipelineId,
        String bundleVersionId);

    Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId);

    <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming);

    Uni<Object> getExecutionResultPayload(String tenantId, String executionId);

    Uni<AwaitCompletionResult> completeAwait(AwaitCompletionCommand command);

    Uni<List<AwaitInteractionRecord>> queryPendingAwaitInteractions(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit);

    Uni<Void> processExecutionWorkItem(ExecutionWorkItem workItem, PipelineTransitionWorker worker);
}
