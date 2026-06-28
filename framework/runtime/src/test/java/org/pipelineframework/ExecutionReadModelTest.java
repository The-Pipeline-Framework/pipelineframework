package org.pipelineframework;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.NotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExecutionReadModelTest {

  @Mock
  private PipelineOrchestratorConfig orchestratorConfig;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private ControlPlaneAdmissionPolicy admissionPolicy;

  private ExecutionInputPolicy executionInputPolicy;
  private JsonTransitionPayloadCodec payloadCodec;
  private ExecutionReadModel readModel;

  @BeforeEach
  void setUp() {
    executionInputPolicy = new ExecutionInputPolicy();
    executionInputPolicy.orchestratorConfig = orchestratorConfig;
    payloadCodec = new JsonTransitionPayloadCodec();
    readModel = new ExecutionReadModel(
        orchestratorConfig,
        executionInputPolicy,
        executionStateStore,
        admissionPolicy,
        () -> "pipeline-a",
        () -> "release-a",
        () -> payloadCodec);
  }

  @Test
  void statusReturnsExecutionStatusDto() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(Optional.of(record("tenant-1", "exec-1", ExecutionStatus.RUNNING))));

    ExecutionStatusDto dto = readModel.getExecutionStatus("tenant-1", "exec-1").await().indefinitely();

    assertEquals("exec-1", dto.executionId());
    assertEquals(ExecutionStatus.RUNNING, dto.status());
    assertEquals(1L, dto.version());
  }

  @Test
  void queueModeDisabledFailsBeforeStoreLookup() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.SYNC);

    IllegalStateException error = assertThrows(IllegalStateException.class, () ->
        readModel.getExecutionStatus("tenant-1", "exec-1").await().indefinitely());

    assertTrue(error.getMessage().contains("Async queue mode is disabled"));
    verify(executionStateStore, never()).getExecution(any(), any());
  }

  @Test
  void admissionDeniedFailsBeforeStoreLookup() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any()))
        .thenReturn(ControlPlaneAdmissionDecision.deny("read-denied", "no reads"));

    ControlPlaneAdmissionException error = assertThrows(ControlPlaneAdmissionException.class, () ->
        readModel.getExecutionStatus("tenant-1", "exec-1").await().indefinitely());

    assertTrue(error.getMessage().contains("read-denied"));
    verify(executionStateStore, never()).getExecution(any(), any());
  }

  @Test
  void missingExecutionThrowsNotFound() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "missing"))
        .thenReturn(Uni.createFrom().item(Optional.empty()));

    NotFoundException error = assertThrows(NotFoundException.class, () ->
        readModel.getExecutionResult("tenant-1", "missing", String.class, false).await().indefinitely());

    assertTrue(error.getMessage().contains("Execution not found: missing"));
  }

  @Test
  void succeededSingleEmptyResultReturnsNull() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-empty"))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded(
            "tenant-1",
            "exec-empty",
            ExecutionResultShape.SINGLE,
            List.of()))));

    Object result = readModel.getExecutionResult("tenant-1", "exec-empty", String.class, false)
        .await().indefinitely();

    assertNull(result);
  }

  @Test
  void succeededSingleEmptyResultProducesEmptyOptionalView() {
    ExecutionResultView view = new ExecutionResultView(succeeded(
        "tenant-1",
        "exec-empty",
        ExecutionResultShape.SINGLE,
        List.of()));

    Optional<Object> result = view.typedOptionalResult(String.class, false, () -> payloadCodec);

    assertTrue(result.isEmpty());
  }

  @Test
  void succeededSingleOneItemReturnsTypedItem() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-one"))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded(
            "tenant-1",
            "exec-one",
            ExecutionResultShape.SINGLE,
            List.of(Map.of("value", "ok"))))));

    TestResult result = (TestResult) readModel.getExecutionResult("tenant-1", "exec-one", TestResult.class, false)
        .await().indefinitely();

    assertEquals("ok", result.value());
  }

  @Test
  void corruptSingleMultipleItemsFails() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-corrupt"))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded(
            "tenant-1",
            "exec-corrupt",
            ExecutionResultShape.SINGLE,
            List.of("a", "b")))));

    IllegalStateException error = assertThrows(IllegalStateException.class, () ->
        readModel.getExecutionResult("tenant-1", "exec-corrupt", String.class, false).await().indefinitely());

    assertTrue(error.getMessage().contains("multiple terminal items"));
  }

  @Test
  void succeededMaterializedMultiReturnsListForStreamingRetrieval() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-multi"))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded(
            "tenant-1",
            "exec-multi",
            ExecutionResultShape.MATERIALIZED_MULTI,
            List.of("a", "b")))));

    Object result = readModel.getExecutionResult("tenant-1", "exec-multi", String.class, true)
        .await().indefinitely();

    assertEquals(List.of("a", "b"), result);
  }

  @Test
  void nonStreamingRetrievalOfMaterializedMultiFails() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-multi"))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded(
            "tenant-1",
            "exec-multi",
            ExecutionResultShape.MATERIALIZED_MULTI,
            List.of("a", "b")))));

    IllegalStateException error = assertThrows(IllegalStateException.class, () ->
        readModel.getExecutionResult("tenant-1", "exec-multi", String.class, false).await().indefinitely());

    assertTrue(error.getMessage().contains("materialized multi result"));
  }

  @Test
  void runningExecutionSaysNotComplete() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-running"))
        .thenReturn(Uni.createFrom().item(Optional.of(record("tenant-1", "exec-running", ExecutionStatus.RUNNING))));

    IllegalStateException error = assertThrows(IllegalStateException.class, () ->
        readModel.getExecutionResult("tenant-1", "exec-running", String.class, false).await().indefinitely());

    assertTrue(error.getMessage().contains("Execution is not complete yet: RUNNING"));
  }

  @Test
  void failedExecutionSaysFinishedWithoutSuccessfulResult() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-failed"))
        .thenReturn(Uni.createFrom().item(Optional.of(record("tenant-1", "exec-failed", ExecutionStatus.FAILED))));

    IllegalStateException error = assertThrows(IllegalStateException.class, () ->
        readModel.getExecutionResult("tenant-1", "exec-failed", String.class, false).await().indefinitely());

    assertTrue(error.getMessage().contains("Execution finished without a successful result: FAILED"));
  }

  @Test
  void dlqExecutionSaysFinishedWithoutSuccessfulResult() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("tenant-1", "exec-dlq"))
        .thenReturn(Uni.createFrom().item(Optional.of(record("tenant-1", "exec-dlq", ExecutionStatus.DLQ))));

    IllegalStateException error = assertThrows(IllegalStateException.class, () ->
        readModel.getExecutionResult("tenant-1", "exec-dlq", String.class, false).await().indefinitely());

    assertTrue(error.getMessage().contains("Execution finished without a successful result: DLQ"));
  }

  @Test
  void typedRetrievalDecodesSerializedTransitionPayload() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    SerializedTransitionPayload serialized = payloadCodec.encode(Map.of("value", "decoded"));
    when(executionStateStore.getExecution("tenant-1", "exec-serialized"))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded(
            "tenant-1",
            "exec-serialized",
            ExecutionResultShape.SINGLE,
            List.of(serialized)))));

    TestResult result = (TestResult) readModel.getExecutionResult(
            "tenant-1",
            "exec-serialized",
            TestResult.class,
            false)
        .await().indefinitely();

    assertEquals("decoded", result.value());
  }

  @Test
  void rawPayloadRetrievalReturnsStoredPayloadWithoutTypedCoercion() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    SerializedTransitionPayload serialized = payloadCodec.encode(Map.of("value", "raw"));
    when(executionStateStore.getExecution("tenant-1", "exec-raw"))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded(
            "tenant-1",
            "exec-raw",
            ExecutionResultShape.SINGLE,
            List.of(serialized)))));

    Object result = readModel.getExecutionResultPayload("tenant-1", "exec-raw").await().indefinitely();

    assertInstanceOf(SerializedTransitionPayload.class, result);
    assertEquals(serialized, result);
  }

  @Test
  void admissionRequestUsesNormalizedTenantAndReleaseIdentity() {
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(orchestratorConfig.defaultTenant()).thenReturn("default-tenant");
    when(admissionPolicy.admit(any())).thenReturn(ControlPlaneAdmissionDecision.allow());
    when(executionStateStore.getExecution("default-tenant", "exec-1"))
        .thenReturn(Uni.createFrom().item(Optional.of(record("default-tenant", "exec-1", ExecutionStatus.RUNNING))));

    readModel.getExecutionStatus(null, "exec-1").await().indefinitely();

    org.mockito.ArgumentCaptor<ControlPlaneAdmissionRequest> request =
        org.mockito.ArgumentCaptor.forClass(ControlPlaneAdmissionRequest.class);
    verify(admissionPolicy).admit(request.capture());
    assertEquals("default-tenant", request.getValue().tenantId());
    assertEquals("pipeline-a", request.getValue().pipelineId());
    assertEquals("release-a", request.getValue().releaseVersion());
    assertFalse(request.getValue().explicitTenant());
  }

  private ExecutionRecord<Object, Object> record(String tenantId, String executionId, ExecutionStatus status) {
    return new ExecutionRecord<>(
        tenantId,
        executionId,
        "key-" + executionId,
        ExecutionResultShape.SINGLE,
        status,
        1L,
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
        2L,
        99999999L);
  }

  private ExecutionRecord<Object, Object> succeeded(
      String tenantId,
      String executionId,
      ExecutionResultShape resultShape,
      List<?> resultPayload) {
    return new ExecutionRecord<>(
        tenantId,
        executionId,
        "key-" + executionId,
        resultShape,
        ExecutionStatus.SUCCEEDED,
        1L,
        0,
        0,
        null,
        0L,
        0L,
        null,
        null,
        null,
        resultPayload,
        null,
        null,
        1L,
        2L,
        99999999L);
  }

  record TestResult(String value) {
  }
}
