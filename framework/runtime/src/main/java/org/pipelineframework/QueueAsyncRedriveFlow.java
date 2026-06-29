package org.pipelineframework;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.NotFoundException;
import org.jboss.logging.Logger;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveResult;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;

class QueueAsyncRedriveFlow {

  private static final Logger LOG = Logger.getLogger(QueueAsyncRedriveFlow.class);

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionInputPolicy executionInputPolicy;
  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final ControlPlaneAdmissionPolicy admissionPolicy;
  private final Supplier<String> pipelineId;
  private final Supplier<String> releaseVersion;

  QueueAsyncRedriveFlow(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionInputPolicy executionInputPolicy,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      ControlPlaneAdmissionPolicy admissionPolicy,
      Supplier<String> pipelineId,
      Supplier<String> releaseVersion) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionInputPolicy = Objects.requireNonNull(executionInputPolicy, "executionInputPolicy must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy must not be null");
    this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId must not be null");
    this.releaseVersion = Objects.requireNonNull(releaseVersion, "releaseVersion must not be null");
  }

  Uni<ExecutionRedriveResult> redrive(
      String tenantId,
      String executionId,
      Long expectedVersion,
      boolean allowFailed,
      String reason) {
    return Uni.createFrom().deferred(() -> {
      if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
        return Uni.createFrom().failure(new IllegalStateException(
            "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
      }
      String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
      Optional<RuntimeException> admissionFailure = admissionFailure(resolvedTenant, tenantId, executionId);
      if (admissionFailure.isPresent()) {
        return Uni.createFrom().failure(admissionFailure.get());
      }
      long now = System.currentTimeMillis();
      return executionStateStore.getExecution(resolvedTenant, executionId)
          .onItem().transformToUni(optional -> accept(
              resolvedTenant,
              executionId,
              optional,
              expectedVersion,
              allowFailed,
              reason,
              now));
    });
  }

  private Uni<ExecutionRedriveResult> accept(
      String tenantId,
      String executionId,
      Optional<ExecutionRecord<Object, Object>> optional,
      Long expectedVersion,
      boolean allowFailed,
      String reason,
      long now) {
    if (optional.isEmpty()) {
      return Uni.createFrom().failure(new NotFoundException("Execution not found: " + executionId));
    }
    ExecutionRedrivePlan plan = ExecutionRedrivePlan.from(optional.get(), expectedVersion, allowFailed, reason);
    return executionStateStore.redriveTerminalExecution(
            tenantId,
            executionId,
            plan.expectedVersion(),
            plan.allowFailed(),
            plan.transitionKey(),
            now)
        .onItem().transformToUni(redriven -> redriven
            .map(record -> enqueue(plan, record))
            .orElseGet(() -> Uni.createFrom().failure(new IllegalStateException(
                "Execution " + executionId + " changed before re-drive could be admitted"))));
  }

  private Uni<ExecutionRedriveResult> enqueue(
      ExecutionRedrivePlan plan,
      ExecutionRecord<Object, Object> record) {
    return workDispatcher.enqueueNow(new ExecutionWorkItem(record.tenantId(), record.executionId()))
        .onFailure().recoverWithUni(failure -> {
          LOG.warnf(
              failure,
              "Re-drive admitted but immediate enqueue failed; due-work sweep will retry tenant=%s executionId=%s stepIndex=%d attempt=%d",
              record.tenantId(),
              record.executionId(),
              record.currentStepIndex(),
              record.attempt());
          return Uni.createFrom().voidItem();
        })
        .onItem().transform(ignored -> {
          LOG.infof(
              "Re-drove execution tenant=%s executionId=%s previousStatus=%s stepIndex=%d attempt=%d",
              record.tenantId(),
              record.executionId(),
              plan.previous().status(),
              record.currentStepIndex(),
              record.attempt());
          return ExecutionRedriveResult.from(plan.previous(), record);
        });
  }

  private Optional<RuntimeException> admissionFailure(
      String resolvedTenant,
      String originalTenant,
      String executionId) {
    ControlPlaneAdmissionDecision decision = admissionPolicy.admit(new ControlPlaneAdmissionRequest(
        resolvedTenant,
        ControlPlaneAdmissionOperation.REDRIVE_EXECUTION,
        pipelineId.get(),
        releaseVersion.get(),
        executionId,
        "api",
        originalTenant != null && !originalTenant.isBlank()));
    return decision.allowed()
        ? Optional.empty()
        : Optional.of(new ControlPlaneAdmissionException(decision));
  }
}
