package org.pipelineframework;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.BadRequestException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

class QueueAsyncSubmissionFlow {

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionInputPolicy executionInputPolicy;
  private final ExecutionResultShapeResolver executionResultShapeResolver;
  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final ControlPlaneAdmissionPolicy admissionPolicy;
  private final Supplier<String> pipelineId;
  private final Supplier<String> contractVersion;
  private final Supplier<String> releaseVersion;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;

  QueueAsyncSubmissionFlow(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionInputPolicy executionInputPolicy,
      ExecutionResultShapeResolver executionResultShapeResolver,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      ControlPlaneAdmissionPolicy admissionPolicy,
      Supplier<String> pipelineId,
      Supplier<String> contractVersion,
      Supplier<String> releaseVersion,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionInputPolicy = Objects.requireNonNull(executionInputPolicy, "executionInputPolicy must not be null");
    this.executionResultShapeResolver =
        Objects.requireNonNull(executionResultShapeResolver, "executionResultShapeResolver must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy must not be null");
    this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId must not be null");
    this.contractVersion = Objects.requireNonNull(contractVersion, "contractVersion must not be null");
    this.releaseVersion = Objects.requireNonNull(releaseVersion, "releaseVersion must not be null");
    this.segmentBoundaryLedger =
        Objects.requireNonNull(segmentBoundaryLedger, "segmentBoundaryLedger must not be null");
  }

  Uni<RunAsyncAcceptedDto> submit(
      Object input,
      String tenantId,
      String idempotencyKey,
      boolean outputStreaming) {
    return submit(
        input,
        tenantId,
        idempotencyKey,
        outputStreaming,
        pipelineId.get(),
        contractVersion.get(),
        releaseVersion.get());
  }

  Uni<RunAsyncAcceptedDto> submit(
      Object input,
      String tenantId,
      String idempotencyKey,
      boolean outputStreaming,
      String pipelineId,
      String contractVersion,
      String releaseVersion) {
    return Uni.createFrom().deferred(() -> {
      Optional<RuntimeException> guardFailure = guardSubmission(outputStreaming);
      if (guardFailure.isPresent()) {
        return Uni.createFrom().failure(guardFailure.get());
      }
      Object executionInput = executionInputPolicy.normalizeExecutionInput(input);
      RuntimeException inputFailure = executionInputPolicy.validateInputShape(executionInput);
      if (inputFailure != null) {
        return Uni.createFrom().failure(inputFailure);
      }
      PipelineRunSubmission submission = new PipelineRunSubmission(
          executionInputPolicy.normalizeTenant(tenantId),
          tenantId != null && !tenantId.isBlank(),
          pipelineId,
          contractVersion,
          releaseVersion,
          idempotencyKey,
          outputStreaming);
      Optional<RuntimeException> admissionFailure = admissionFailure(submission);
      if (admissionFailure.isPresent()) {
        return Uni.createFrom().failure(admissionFailure.get());
      }
      long now = System.currentTimeMillis();
      long ttlEpochS = ttlEpochS(now);
      return executionInputPolicy.resolveExecutionInputPayload(executionInput)
          .onItem().transformToUni(snapshot -> createPlan(submission, snapshot, now, ttlEpochS))
          .onItem().transform(PipelineRunSubmissionPlan::createCommand)
          .onItem().transformToUni(command -> executionStateStore.createOrGetExecution(command)
              .onItem().transformToUni(created -> accept(created, command, now)));
    });
  }

  private Optional<RuntimeException> guardSubmission(boolean outputStreaming) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Optional.of(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    if (outputStreaming) {
      return Optional.of(new IllegalStateException(
          "Async queue mode does not support streaming pipeline outputs yet."));
    }
    return Optional.empty();
  }

  private Uni<PipelineRunSubmissionPlan> createPlan(
      PipelineRunSubmission submission,
      ExecutionInputSnapshot snapshot,
      long now,
      long ttlEpochS) {
    String executionKey;
    try {
      executionKey = executionInputPolicy.resolveExecutionKey(
          submission.tenantId(),
          snapshot.payload(),
          submission.idempotencyKey());
    } catch (IllegalArgumentException e) {
      return Uni.createFrom().failure(new BadRequestException(e.getMessage()));
    }
    ExecutionResultShape resultShape = executionResultShapeResolver.resolve();
    try {
      return Uni.createFrom().item(new PipelineRunSubmissionPlan(
          submission,
          snapshot,
          executionKey,
          resultShape,
          now,
          ttlEpochS));
    } catch (IllegalArgumentException e) {
      return Uni.createFrom().failure(new BadRequestException(e.getMessage()));
    }
  }

  private Uni<RunAsyncAcceptedDto> accept(
      CreateExecutionResult created,
      ExecutionCreateCommand command,
      long now) {
    Uni<Void> recordSubmitted = created.duplicate()
        ? Uni.createFrom().voidItem()
        : segmentBoundaryLedger.get().recordRunSubmitted(created, command, now);
    Uni<Void> enqueue = created.duplicate()
        ? Uni.createFrom().voidItem()
        : workDispatcher.enqueueNow(new ExecutionWorkItem(
            created.record().tenantId(),
            created.record().executionId()));
    return recordSubmitted
        .chain(() -> enqueue)
        .onItem().transform(ignored -> new RunAcceptance(created, now).toDto());
  }

  private Optional<RuntimeException> admissionFailure(PipelineRunSubmission submission) {
    ControlPlaneAdmissionDecision decision = admissionPolicy.admit(submission.admissionRequest());
    return decision.allowed()
        ? Optional.empty()
        : Optional.of(new ControlPlaneAdmissionException(decision));
  }

  private long ttlEpochS(long nowEpochMs) {
    return Instant.ofEpochMilli(nowEpochMs)
        .plus(Duration.ofDays(Math.max(1, orchestratorConfig.executionTtlDays())))
        .getEpochSecond();
  }
}
