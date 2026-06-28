package org.pipelineframework;

import java.util.List;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncSubmissionFlowTest {

  private ExecutionInputPolicy inputPolicy;
  private QueueAsyncSubmissionFlow flow;

  @Mock
  private PipelineOrchestratorConfig orchestratorConfig;

  @Mock
  private ExecutionResultShapeResolver resultShapeResolver;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private WorkDispatcher workDispatcher;

  @Mock
  private ControlPlaneAdmissionPolicy admissionPolicy;

  @Mock
  private SegmentBoundaryLedger segmentBoundaryLedger;

  @BeforeEach
  void setUp() {
    inputPolicy = new ExecutionInputPolicy();
    inputPolicy.orchestratorConfig = orchestratorConfig;
    lenient().when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    lenient().when(orchestratorConfig.defaultTenant()).thenReturn("default");
    lenient().when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY);
    lenient().when(orchestratorConfig.executionTtlDays()).thenReturn(1);
    lenient().when(resultShapeResolver.resolve()).thenReturn(ExecutionResultShape.SINGLE);
    lenient().when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    lenient().when(segmentBoundaryLedger.recordRunSubmitted(any(), any(), org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(Uni.createFrom().voidItem());
    flow = newFlow(inputPolicy, () -> segmentBoundaryLedger);
  }

  @Test
  void queueModeDisabledFailsBeforeStoreAccess() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.SYNC);

    Uni<?> result = flow.submit("input", "tenant-1", "idem-1", false);

    assertThrows(IllegalStateException.class, () -> result.await().indefinitely());
    verify(executionStateStore, never()).createOrGetExecution(any());
    verify(admissionPolicy, never()).admit(any());
  }

  @Test
  void streamingOutputFlagIsRejected() {
    Uni<?> result = flow.submit("input", "tenant-1", "idem-1", true);

    IllegalStateException error = assertThrows(IllegalStateException.class, () -> result.await().indefinitely());

    assertTrue(error.getMessage().contains("does not support streaming pipeline outputs"));
    verify(executionStateStore, never()).createOrGetExecution(any());
  }

  @Test
  void invalidInputShapeFailsBeforeAdmissionAndStoreAccess() {
    ExecutionInputPolicy rejectingPolicy = mock(ExecutionInputPolicy.class);
    when(rejectingPolicy.normalizeExecutionInput("input")).thenReturn("bad-shape");
    when(rejectingPolicy.validateInputShape("bad-shape")).thenReturn(new IllegalArgumentException("bad input"));
    QueueAsyncSubmissionFlow rejectingFlow = newFlow(rejectingPolicy, () -> segmentBoundaryLedger);

    Uni<?> result = rejectingFlow.submit("input", "tenant-1", "idem-1", false);

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> result.await().indefinitely());
    assertEquals("bad input", error.getMessage());
    verify(admissionPolicy, never()).admit(any());
    verify(executionStateStore, never()).createOrGetExecution(any());
  }

  @Test
  void deniedTenantFailsBeforeSnapshotAndStoreAccess() {
    ExecutionInputPolicy policy = mock(ExecutionInputPolicy.class);
    Object normalized = Uni.createFrom().item("input");
    when(policy.normalizeExecutionInput("input")).thenReturn(normalized);
    when(policy.validateInputShape(normalized)).thenReturn(null);
    when(policy.normalizeTenant("tenant-denied")).thenReturn("tenant-denied");
    when(admissionPolicy.admit(any()))
        .thenReturn(ControlPlaneAdmissionDecision.deny("TENANT_NOT_ALLOWED", "tenant denied"));
    QueueAsyncSubmissionFlow deniedFlow = newFlow(policy, () -> segmentBoundaryLedger);

    Uni<?> result = deniedFlow.submit("input", "tenant-denied", "idem-1", false);

    ControlPlaneAdmissionException error = assertThrows(
        ControlPlaneAdmissionException.class,
        () -> result.await().indefinitely());
    assertTrue(error.getMessage().contains("TENANT_NOT_ALLOWED"));
    verify(policy, never()).resolveExecutionInputPayload(any());
    verify(executionStateStore, never()).createOrGetExecution(any());
  }

  @Test
  void uniListPayloadIsPreservedAsSingleInputItem() {
    when(executionStateStore.createOrGetExecution(any()))
        .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record("exec-1", "key-1"), true)));

    flow.submit(List.of("a", "b"), "tenant-1", null, false).await().indefinitely();

    ArgumentCaptor<ExecutionCreateCommand> captor = ArgumentCaptor.forClass(ExecutionCreateCommand.class);
    verify(executionStateStore).createOrGetExecution(captor.capture());
    assertInstanceOf(ExecutionInputSnapshot.class, captor.getValue().inputPayload());
    ExecutionInputSnapshot snapshot = (ExecutionInputSnapshot) captor.getValue().inputPayload();
    assertEquals(ExecutionInputShape.UNI, snapshot.shape());
    assertEquals(List.of("a", "b"), snapshot.payload());
  }

  @Test
  void multiInputMaterializesToMultiSnapshot() {
    when(executionStateStore.createOrGetExecution(any()))
        .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record("exec-1", "key-1"), true)));

    flow.submit(Multi.createFrom().items("x", "y"), "tenant-1", null, false).await().indefinitely();

    ArgumentCaptor<ExecutionCreateCommand> captor = ArgumentCaptor.forClass(ExecutionCreateCommand.class);
    verify(executionStateStore).createOrGetExecution(captor.capture());
    ExecutionInputSnapshot snapshot = (ExecutionInputSnapshot) captor.getValue().inputPayload();
    assertEquals(ExecutionInputShape.MULTI, snapshot.shape());
    assertEquals(List.of("x", "y"), snapshot.payload());
  }

  @Test
  void resolvedResultShapeIsPersisted() {
    when(resultShapeResolver.resolve()).thenReturn(ExecutionResultShape.MATERIALIZED_MULTI);
    when(executionStateStore.createOrGetExecution(any()))
        .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
            record("exec-1", "key-1", ExecutionResultShape.MATERIALIZED_MULTI),
            true)));

    flow.submit("input", "tenant-1", null, false).await().indefinitely();

    ArgumentCaptor<ExecutionCreateCommand> captor = ArgumentCaptor.forClass(ExecutionCreateCommand.class);
    verify(executionStateStore).createOrGetExecution(captor.capture());
    assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, captor.getValue().resultShape());
  }

  @Test
  void duplicateExecutionSkipsLedgerAndEnqueue() {
    when(executionStateStore.createOrGetExecution(any()))
        .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record("exec-1", "key-1"), true)));

    RunAsyncAcceptedDto dto = flow.submit("input", "tenant-1", "idem-1", false).await().indefinitely();

    assertEquals("exec-1", dto.executionId());
    assertTrue(dto.duplicate());
    verify(segmentBoundaryLedger, never()).recordRunSubmitted(any(), any(), org.mockito.ArgumentMatchers.anyLong());
    verify(workDispatcher, never()).enqueueNow(any());
  }

  @Test
  void newExecutionAppendsRunSubmittedThenEnqueuesInitialWorkThenReturnsAcceptedDto() {
    ExecutionRecord<Object, Object> record = record("exec-1", "key-1");
    when(executionStateStore.createOrGetExecution(any()))
        .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record, false)));
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

    RunAsyncAcceptedDto dto = flow.submit("input", "tenant-1", "idem-1", false).await().indefinitely();

    assertEquals("exec-1", dto.executionId());
    assertFalse(dto.duplicate());
    assertEquals("/pipeline/executions/exec-1", dto.statusUrl());
    InOrder order = inOrder(segmentBoundaryLedger, workDispatcher);
    order.verify(segmentBoundaryLedger).recordRunSubmitted(
        any(),
        any(),
        org.mockito.ArgumentMatchers.anyLong());
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
  }

  @Test
  void admissionRequestUsesNormalizedIdentity() {
    when(executionStateStore.createOrGetExecution(any()))
        .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record("exec-1", "key-1"), true)));

    flow.submit("input", "  ", "idem-1", false).await().indefinitely();

    ArgumentCaptor<ControlPlaneAdmissionRequest> captor = ArgumentCaptor.forClass(ControlPlaneAdmissionRequest.class);
    verify(admissionPolicy).admit(captor.capture());
    ControlPlaneAdmissionRequest request = captor.getValue();
    assertEquals("default", request.tenantId());
    assertEquals("pipeline-a", request.pipelineId());
    assertEquals("release-a", request.releaseVersion());
    assertFalse(request.explicitTenant());
  }

  private QueueAsyncSubmissionFlow newFlow(
      ExecutionInputPolicy policy,
      Supplier<SegmentBoundaryLedger> ledger) {
    return new QueueAsyncSubmissionFlow(
        orchestratorConfig,
        policy,
        resultShapeResolver,
        executionStateStore,
        workDispatcher,
        admissionPolicy,
        () -> "pipeline-a",
        () -> "contract-a",
        () -> "release-a",
        ledger);
  }

  private ExecutionRecord<Object, Object> record(String executionId, String executionKey) {
    return record(executionId, executionKey, ExecutionResultShape.SINGLE);
  }

  private ExecutionRecord<Object, Object> record(
      String executionId,
      String executionKey,
      ExecutionResultShape resultShape) {
    return new ExecutionRecord<>(
        "tenant-1",
        executionId,
        executionKey,
        "pipeline-a",
        "contract-a",
        "release-a",
        resultShape,
        ExecutionStatus.QUEUED,
        0L,
        0,
        0,
        null,
        0L,
        0L,
        null,
        null,
        null,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }
}
