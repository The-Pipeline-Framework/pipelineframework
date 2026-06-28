package org.pipelineframework;

import java.util.List;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncSweepFlowTest {

  private QueueAsyncSweepFlow flow;

  @Mock
  private PipelineOrchestratorConfig orchestratorConfig;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private WorkDispatcher workDispatcher;

  @Mock
  private AwaitTimeoutFlow awaitTimeoutFlow;

  @BeforeEach
  void setUp() {
    when(orchestratorConfig.sweepLimit()).thenReturn(100);
    flow = new QueueAsyncSweepFlow(
        orchestratorConfig,
        executionStateStore,
        workDispatcher,
        awaitTimeoutFlow);
  }

  @Test
  void sweepRunsTimeoutsThenEnqueuesEveryDueExecution() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(
        record("tenant-b", "exec-b", 20L),
        record("tenant-b", "exec-a", 10L),
        record("tenant-a", "exec-c", 10L))));
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

    flow.sweepOnce(1000L).await().indefinitely();

    InOrder order = inOrder(awaitTimeoutFlow, executionStateStore, workDispatcher);
    order.verify(awaitTimeoutFlow).sweepTimedOut(1000L, 100);
    order.verify(executionStateStore).findDueExecutions(1000L, 100);
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-a", "exec-c"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-a"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b"));
  }

  @Test
  void emptyDueBatchDoesNotDispatch() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of()));

    flow.sweepOnce(1000L).await().indefinitely();

    verify(workDispatcher, never()).enqueueNow(any());
  }

  @Test
  void enqueueFailureNamesExecutionId() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(
        record("tenant-a", "exec-a"),
        record("tenant-b", "exec-b"))));
    when(workDispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-a")))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("dispatcher down")));
    when(workDispatcher.enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b")))
        .thenReturn(Uni.createFrom().voidItem());

    IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> flow.sweepOnce(1000L).await().indefinitely());

    assertTrue(error.getMessage().contains("Failed to re-dispatch due execution exec-a"));
    InOrder order = inOrder(workDispatcher);
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-a", "exec-a"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b"));
  }

  @Test
  void scheduledSweepDoesNotThrowFromCallerWhenSubscriptionFails() {
    when(awaitTimeoutFlow.sweepTimedOut(anyLong(), anyInt()))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("timeout store down")));

    assertDoesNotThrow(() -> flow.sweepDueExecutions());

    verify(awaitTimeoutFlow).sweepTimedOut(anyLong(), anyInt());
  }

  private static ExecutionRecord<Object, Object> record(String tenantId, String executionId) {
    return record(tenantId, executionId, 1L);
  }

  private static ExecutionRecord<Object, Object> record(String tenantId, String executionId, long nextDueEpochMs) {
    return new ExecutionRecord<>(
        tenantId,
        executionId,
        executionId + "-key",
        ExecutionResultShape.SINGLE,
        ExecutionStatus.WAIT_RETRY,
        1L,
        2,
        1,
        null,
        0L,
        nextDueEpochMs,
        null,
        "input",
        null,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }
}
