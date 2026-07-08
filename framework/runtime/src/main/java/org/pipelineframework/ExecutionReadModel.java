package org.pipelineframework;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.NotFoundException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;

class ExecutionReadModel {

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionInputPolicy executionInputPolicy;
  private final ExecutionStateStore executionStateStore;
  private final ControlPlaneAdmissionPolicy admissionPolicy;
  private final Supplier<String> pipelineId;
  private final Supplier<String> releaseVersion;
  private final Supplier<TransitionPayloadCodec> payloadCodec;

  ExecutionReadModel(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionInputPolicy executionInputPolicy,
      ExecutionStateStore executionStateStore,
      ControlPlaneAdmissionPolicy admissionPolicy,
      Supplier<String> pipelineId,
      Supplier<String> releaseVersion,
      Supplier<TransitionPayloadCodec> payloadCodec) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionInputPolicy = Objects.requireNonNull(executionInputPolicy, "executionInputPolicy must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy must not be null");
    this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId must not be null");
    this.releaseVersion = Objects.requireNonNull(releaseVersion, "releaseVersion must not be null");
    this.payloadCodec = Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
  }

  Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
    return readExecution(tenantId, executionId, ControlPlaneAdmissionOperation.GET_EXECUTION_STATUS)
        .onItem().transform(record -> new ExecutionResultView(record).statusDto());
  }

  @SuppressWarnings("unchecked")
  <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
    return readExecution(tenantId, executionId, ControlPlaneAdmissionOperation.GET_EXECUTION_RESULT)
        .onItem().transform(record ->
            (T) new ExecutionResultView(record).typedResult(outputType, outputStreaming, payloadCodec));
  }

  Uni<Object> getExecutionResultPayload(String tenantId, String executionId) {
    return readExecution(tenantId, executionId, ControlPlaneAdmissionOperation.GET_EXECUTION_RESULT)
        .onItem().transform(record -> new ExecutionResultView(record).rawPayload());
  }

  private Uni<ExecutionRecord<Object, Object>> readExecution(
      String tenantId,
      String executionId,
      ControlPlaneAdmissionOperation operation) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    Optional<RuntimeException> admissionFailure = admissionFailure(new ControlPlaneAdmissionRequest(
        resolvedTenant,
        operation,
        pipelineId.get(),
        releaseVersion.get(),
        executionId,
        "api",
        tenantId != null && !tenantId.isBlank()));
    if (admissionFailure.isPresent()) {
      return Uni.createFrom().failure(admissionFailure.get());
    }
    return executionStateStore.getExecution(resolvedTenant, executionId)
        .onItem().transform(optional -> requireExecution(optional, executionId));
  }

  private Optional<RuntimeException> admissionFailure(ControlPlaneAdmissionRequest request) {
    ControlPlaneAdmissionDecision decision = admissionPolicy.admit(request);
    return decision.allowed()
        ? Optional.empty()
        : Optional.of(new ControlPlaneAdmissionException(decision));
  }

  private ExecutionRecord<Object, Object> requireExecution(
      Optional<ExecutionRecord<Object, Object>> record,
      String executionId) {
    return record.orElseThrow(() -> new NotFoundException("Execution not found: " + executionId));
  }
}
