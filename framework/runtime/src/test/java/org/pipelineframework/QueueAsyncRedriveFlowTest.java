package org.pipelineframework;

import java.util.Optional;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.NotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveResult;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncRedriveFlowTest {

  private QueueAsyncRedriveFlow flow;
  private ExecutionInputPolicy inputPolicy;

  @Mock
  private PipelineOrchestratorConfig orchestratorConfig;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private WorkDispatcher workDispatcher;

  @Mock
  private ControlPlaneAdmissionPolicy admissionPolicy;

  @BeforeEach
  void setUp() {
    inputPolicy = new ExecutionInputPolicy();
    inputPolicy.orchestratorConfig = orchestratorConfig;
    lenient().when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    lenient().when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    flow = new QueueAsyncRedriveFlow(
        orchestratorConfig,
        inputPolicy,
        executionStateStore,
        workDispatcher,
        admissionPolicy,
        () -> "pipeline-a",
        () -> "release-a");
  }

  @Test
  void redriveUpdatesStoreThenEnqueuesOriginalExecutionId() {
    ExecutionRecord<Object, Object> terminal = record(ExecutionStatus.DLQ, 4L, 2);
    ExecutionRecord<Object, Object> redriven = record(ExecutionStatus.QUEUED, 5L, 3);
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(Optional.of(terminal)));
    when(executionStateStore.redriveTerminalExecution(
            eq("tenant-1"),
            eq("exec-1"),
            eq(4L),
            eq(false),
            eq("redrive:exec-1:4"),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(redriven)));
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

    ExecutionRedriveResult result = flow.redrive("tenant-1", "exec-1", null, false, "operator retry")
        .await().indefinitely();

    assertEquals("exec-1", result.executionId());
    assertEquals(ExecutionStatus.DLQ, result.previousStatus());
    assertEquals(ExecutionStatus.QUEUED, result.status());
    InOrder order = inOrder(executionStateStore, workDispatcher);
    order.verify(executionStateStore).redriveTerminalExecution(
        eq("tenant-1"),
        eq("exec-1"),
        eq(4L),
        eq(false),
        eq("redrive:exec-1:4"),
        anyLong());
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
  }

  @Test
  void queueModeDisabledFailsBeforeStoreAccess() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.SYNC);

    assertThrows(IllegalStateException.class, () ->
        flow.redrive("tenant-1", "exec-1", null, false, "retry").await().indefinitely());

    verify(admissionPolicy, never()).admit(any());
    verify(executionStateStore, never()).getExecution(any(), any());
    verify(workDispatcher, never()).enqueueNow(any());
  }

  @Test
  void enqueueFailureReturnsAdmittedRedriveBecauseSweepCanRecoverDueRecord() {
    ExecutionRecord<Object, Object> terminal = record(ExecutionStatus.DLQ, 4L, 2);
    ExecutionRecord<Object, Object> redriven = record(ExecutionStatus.QUEUED, 5L, 3);
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(Optional.of(terminal)));
    when(executionStateStore.redriveTerminalExecution(
            eq("tenant-1"),
            eq("exec-1"),
            eq(4L),
            eq(false),
            eq("redrive:exec-1:4"),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(redriven)));
    when(workDispatcher.enqueueNow(any()))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("dispatcher down")));

    ExecutionRedriveResult result = flow.redrive("tenant-1", "exec-1", null, false, "operator retry")
        .await().indefinitely();

    assertEquals(ExecutionStatus.QUEUED, result.status());
    verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
  }

  @Test
  void deniedAdmissionFailsBeforeStoreAccess() {
    when(admissionPolicy.admit(any()))
        .thenReturn(ControlPlaneAdmissionDecision.deny("TENANT_NOT_ALLOWED", "no"));

    assertThrows(ControlPlaneAdmissionException.class, () ->
        flow.redrive("tenant-1", "exec-1", null, false, "retry").await().indefinitely());

    verify(executionStateStore, never()).getExecution(any(), any());
  }

  @Test
  void missingExecutionFailsBeforeMutation() {
    when(executionStateStore.getExecution("tenant-1", "missing"))
        .thenReturn(Uni.createFrom().item(Optional.empty()));

    NotFoundException error = assertThrows(NotFoundException.class, () ->
        flow.redrive("tenant-1", "missing", null, false, "retry").await().indefinitely());

    assertTrue(error.getMessage().contains("Execution not found: missing"));
    verify(executionStateStore, never()).redriveTerminalExecution(
        any(), any(), anyLong(), org.mockito.ArgumentMatchers.anyBoolean(), any(), anyLong());
  }

  private static ExecutionRecord<Object, Object> record(
      ExecutionStatus status,
      long version,
      int attempt) {
    return new ExecutionRecord<>(
        "tenant-1",
        "exec-1",
        "key-1",
        "pipeline-a",
        "contract-a",
        "release-a",
        ExecutionResultShape.SINGLE,
        status,
        version,
        2,
        attempt,
        null,
        0L,
        0L,
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
