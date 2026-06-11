package org.pipelineframework;

import java.util.List;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalPipelineControlPlaneTest {

  private LocalPipelineControlPlane controlPlane;

  @Mock
  private QueueAsyncCoordinator queueAsyncCoordinator;

  @Mock
  private PipelineTransitionWorker transitionWorker;

  @BeforeEach
  void setUp() {
    controlPlane = new LocalPipelineControlPlane();
    controlPlane.queueAsyncCoordinator = queueAsyncCoordinator;
  }

  @Test
  void executePipelineAsyncDelegatesToCoordinator() {
    RunAsyncAcceptedDto expected = new RunAsyncAcceptedDto("exec-1", false, "/pipeline/executions/exec-1", 10L);
    when(queueAsyncCoordinator.executePipelineAsync("input", "tenant-1", "idem-1", false))
        .thenReturn(Uni.createFrom().item(expected));

    RunAsyncAcceptedDto actual = controlPlane.executePipelineAsync("input", "tenant-1", "idem-1", false)
        .await().indefinitely();

    assertEquals("exec-1", actual.executionId());
    verify(queueAsyncCoordinator).executePipelineAsync("input", "tenant-1", "idem-1", false);
  }

  @Test
  void executionStatusAndResultDelegateToCoordinator() {
    ExecutionStatusDto status = new ExecutionStatusDto("exec-2", null, 0, 0, 1L, 0L, 0L, null, null);
    when(queueAsyncCoordinator.getExecutionStatus("tenant-1", "exec-2"))
        .thenReturn(Uni.createFrom().item(status));
    when(queueAsyncCoordinator.getExecutionResult("tenant-1", "exec-2", String.class, false))
        .thenReturn(Uni.createFrom().item("result"));

    assertEquals("exec-2", controlPlane.getExecutionStatus("tenant-1", "exec-2")
        .await().indefinitely().executionId());
    assertEquals("result", controlPlane.getExecutionResult("tenant-1", "exec-2", String.class, false)
        .await().indefinitely());

    verify(queueAsyncCoordinator).getExecutionStatus("tenant-1", "exec-2");
    verify(queueAsyncCoordinator).getExecutionResult("tenant-1", "exec-2", String.class, false);
  }

  @Test
  void awaitCompletionAndPendingQueryDelegateToCoordinator() {
    AwaitInteractionRecord record = awaitRecord("tenant-1", "exec-3", "interaction-1");
    AwaitCompletionCommand command = new AwaitCompletionCommand(
        "tenant-1", "interaction-1", null, null, "payload", "alice", 10L);
    AwaitCompletionResult completion = new AwaitCompletionResult(record, false);
    when(queueAsyncCoordinator.completeAwait(command)).thenReturn(Uni.createFrom().item(completion));
    when(queueAsyncCoordinator.queryPendingAwaitInteractions("tenant-1", null, "kitchen", "await-decision", 10))
        .thenReturn(Uni.createFrom().item(List.of(record)));

    assertEquals("interaction-1", controlPlane.completeAwait(command)
        .await().indefinitely().record().interactionId());
    assertEquals(1, controlPlane.queryPendingAwaitInteractions("tenant-1", null, "kitchen", "await-decision", 10)
        .await().indefinitely().size());

    verify(queueAsyncCoordinator).completeAwait(command);
    verify(queueAsyncCoordinator).queryPendingAwaitInteractions("tenant-1", null, "kitchen", "await-decision", 10);
  }

  @Test
  void processExecutionWorkItemDelegatesToCoordinatorWithWorker() {
    ExecutionWorkItem workItem = new ExecutionWorkItem("tenant-1", "exec-4");
    when(queueAsyncCoordinator.processExecutionWorkItem(eq(workItem), same(transitionWorker)))
        .thenReturn(Uni.createFrom().voidItem());

    controlPlane.processExecutionWorkItem(workItem, transitionWorker).await().indefinitely();

    verify(queueAsyncCoordinator).processExecutionWorkItem(eq(workItem), same(transitionWorker));
  }

  private static AwaitInteractionRecord awaitRecord(String tenantId, String executionId, String interactionId) {
    return new AwaitInteractionRecord(
        tenantId,
        executionId,
        "await-decision",
        1,
        String.class.getName(),
        interactionId,
        "correlation-" + interactionId,
        null,
        "idempotency-" + interactionId,
        1L,
        AwaitInteractionStatus.COMPLETED,
        "request",
        "response",
        "unit-" + interactionId,
        null,
        "alice",
        null,
        "kitchen",
        "interaction-api",
        java.util.Map.of(),
        100L,
        1L,
        2L,
        1000L);
  }
}
