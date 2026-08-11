package org.pipelineframework;

import java.util.Optional;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionAdmissionFailures;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCompletionTenantMismatchException;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

/**
 * Admits external await completions into the durable boundary model.
 */
class AwaitBoundaryAdmission {

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionInputPolicy executionInputPolicy;
  private final ControlPlaneAdmissionPolicy admissionPolicy;
  private final AwaitCoordinator awaitCoordinator;
  private final AwaitLiveCompletionRegistry awaitLiveCompletionRegistry;
  private final Supplier<String> pipelineId;
  private final Supplier<String> releaseVersion;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final AwaitContinuations continuations;

  AwaitBoundaryAdmission(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionInputPolicy executionInputPolicy,
      ControlPlaneAdmissionPolicy admissionPolicy,
      AwaitCoordinator awaitCoordinator,
      AwaitLiveCompletionRegistry awaitLiveCompletionRegistry,
      Supplier<String> pipelineId,
      Supplier<String> releaseVersion,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      AwaitContinuations continuations) {
    this.orchestratorConfig = orchestratorConfig;
    this.executionInputPolicy = executionInputPolicy;
    this.admissionPolicy = admissionPolicy;
    this.awaitCoordinator = awaitCoordinator;
    this.awaitLiveCompletionRegistry = awaitLiveCompletionRegistry;
    this.pipelineId = pipelineId;
    this.releaseVersion = releaseVersion;
    this.segmentBoundaryLedger = segmentBoundaryLedger;
    this.continuations = continuations;
  }

  Uni<AwaitCompletionResult> complete(
      AwaitCompletionCommand command,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return Uni.createFrom().deferred(() -> {
      if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
        return Uni.createFrom().failure(new IllegalStateException(
            "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
      }
      AwaitCompletionCommand normalized = normalize(command);
      Optional<RuntimeException> admissionFailure = admissionFailure(normalized, command.tenantId());
      if (admissionFailure.isPresent()) {
        return Uni.createFrom().failure(admissionFailure.get());
      }
      return awaitCoordinator.complete(normalized)
          .onFailure(failure -> !AwaitCompletionAdmissionFailures.isDeterministic(failure))
          .transform(failure -> new IllegalStateException(
              "Failed completing await interaction tenant=" + normalized.tenantId()
                  + ", interactionId=" + normalized.interactionId()
                  + ", correlationId=" + normalized.correlationId(),
              failure))
          .onItem().transformToUni(result -> validateTenant(result, normalized)
              .onItem().transformToUni(validated -> route(validated, normalized, itemContinuationHandler)));
    });
  }

  private Uni<AwaitCompletionResult> route(
      AwaitCompletionResult validated,
      AwaitCompletionCommand normalized,
      AwaitItemContinuationHandler itemContinuationHandler) {
    if (validated.record().status() != AwaitInteractionStatus.COMPLETED) {
      return routeTerminalInteraction(validated, normalized, itemContinuationHandler);
    }
    return signalLiveAwaitCompletion(validated.record())
        .onItem().transformToUni(liveAccepted -> {
          Uni<AwaitCompletionResult> handoff = liveAccepted
              ? segmentBoundaryLedger.get()
                  .recordBoundaryCompletionAdmitted(validated.record(), normalized.nowEpochMs())
                  .replaceWith(validated)
              : awaitCoordinator.recordCompletion(validated.record(), normalized.nowEpochMs())
                  .onItem().transformToUni(unit -> segmentBoundaryLedger.get()
                      .recordBoundaryCompletionAdmitted(validated.record(), normalized.nowEpochMs())
                      .chain(() -> continuations.afterRecordedCompletion(
                          validated,
                          unit,
                          itemContinuationHandler,
                          normalized.nowEpochMs())));
          if (!awaitCoordinator.admissionEnabled()) {
            return handoff;
          }
          return awaitCoordinator.releaseAdmission(validated.record()).chain(() -> handoff);
        });
  }

  private Uni<AwaitCompletionResult> routeTerminalInteraction(
      AwaitCompletionResult validated,
      AwaitCompletionCommand normalized,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return awaitCoordinator.recordCompletion(validated.record(), normalized.nowEpochMs())
        .onItem().transformToUni(unit -> segmentBoundaryLedger.get()
            .recordBoundaryCompletionAdmitted(validated.record(), normalized.nowEpochMs())
            .chain(() -> signalLiveAwaitCompletion(validated.record()))
            .onItem().transformToUni(liveAccepted -> {
              Uni<AwaitCompletionResult> handoff = liveAccepted
                  ? Uni.createFrom().item(validated)
                  : continuations.afterRecordedCompletion(
                      validated,
                      unit,
                      itemContinuationHandler,
                      normalized.nowEpochMs());
              if (!awaitCoordinator.admissionEnabled()) {
                return handoff;
              }
              return awaitCoordinator.releaseAdmission(validated.record()).chain(() -> handoff);
            }));
  }

  private AwaitCompletionCommand normalize(AwaitCompletionCommand command) {
    return new AwaitCompletionCommand(
        executionInputPolicy.normalizeTenant(command.tenantId()),
        command.interactionId(),
        command.correlationId(),
        command.resumeToken(),
        command.idempotencyKey(),
        command.responsePayload(),
        command.actor(),
        command.nowEpochMs());
  }

  private Optional<RuntimeException> admissionFailure(AwaitCompletionCommand normalized, String originalTenant) {
    ControlPlaneAdmissionDecision decision = admissionPolicy.admit(new ControlPlaneAdmissionRequest(
        normalized.tenantId(),
        ControlPlaneAdmissionOperation.COMPLETE_AWAIT,
        pipelineId.get(),
        releaseVersion.get(),
        null,
        "api",
        explicitTenant(originalTenant)));
    return decision.allowed()
        ? Optional.empty()
        : Optional.of(new ControlPlaneAdmissionException(decision));
  }

  private Uni<AwaitCompletionResult> validateTenant(
      AwaitCompletionResult result,
      AwaitCompletionCommand command) {
    if (!command.tenantId().equals(result.record().tenantId())) {
      return Uni.createFrom().failure(new AwaitCompletionTenantMismatchException(
          "Await completion tenant mismatch: command tenant=" + command.tenantId()
              + ", record tenant=" + result.record().tenantId()));
    }
    return Uni.createFrom().item(result);
  }

  private Uni<Boolean> signalLiveAwaitCompletion(AwaitInteractionRecord record) {
    if (awaitLiveCompletionRegistry == null) {
      return Uni.createFrom().item(false);
    }
    return awaitLiveCompletionRegistry.signal(record);
  }

  private static boolean explicitTenant(String tenantId) {
    return tenantId != null && !tenantId.isBlank();
  }
}
