package org.pipelineframework;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitThrowableSupport;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.ControlPlaneTransitionAdmission;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

/**
 * Reactive queue-async segment flow: claim, run, plan, and commit one synchronous segment.
 */
class QueueAsyncSegmentPipeline {

  private static final Logger LOG = Logger.getLogger(QueueAsyncSegmentPipeline.class);

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final AwaitCoordinator awaitCoordinator;
  private final TransitionWorkerExecutor transitionWorkerExecutor;
  private final ControlPlaneAdmissionPolicy admissionPolicy;
  private final Supplier<TransitionPayloadCodec> payloadCodec;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;
  private final Supplier<Duration> saturatedDelay;
  private final IntSupplier pipelineStepCount;
  private final SegmentCommitEffects segmentCommitEffects;
  private final String queueWorkerId;

  QueueAsyncSegmentPipeline(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitCoordinator awaitCoordinator,
      TransitionWorkerExecutor transitionWorkerExecutor,
      ControlPlaneAdmissionPolicy admissionPolicy,
      Supplier<TransitionPayloadCodec> payloadCodec,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger,
      Supplier<Duration> saturatedDelay,
      IntSupplier pipelineStepCount,
      SegmentCommitEffects segmentCommitEffects,
      String queueWorkerId) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.awaitCoordinator = Objects.requireNonNull(awaitCoordinator, "awaitCoordinator must not be null");
    this.transitionWorkerExecutor = Objects.requireNonNull(transitionWorkerExecutor, "transitionWorkerExecutor must not be null");
    this.admissionPolicy = Objects.requireNonNull(admissionPolicy, "admissionPolicy must not be null");
    this.payloadCodec = Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
    this.segmentBoundaryLedger = Objects.requireNonNull(segmentBoundaryLedger, "segmentBoundaryLedger must not be null");
    this.saturatedDelay = Objects.requireNonNull(saturatedDelay, "saturatedDelay must not be null");
    this.pipelineStepCount = Objects.requireNonNull(pipelineStepCount, "pipelineStepCount must not be null");
    this.segmentCommitEffects = Objects.requireNonNull(segmentCommitEffects, "segmentCommitEffects must not be null");
    this.queueWorkerId = queueWorkerId == null || queueWorkerId.isBlank() ? "worker-local" : queueWorkerId;
  }

  Uni<Void> process(
      ExecutionWorkItem workItem,
      PipelineTransitionWorker worker,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return Uni.createFrom().deferred(() -> {
      Optional<TransitionWorkerExecutor.TransitionAdmission> admission = transitionWorkerExecutor.tryAdmit();
      if (admission.isEmpty()) {
        return workDispatcher.enqueueDelayed(workItem, saturatedDelay.get());
      }
      TransitionWorkerExecutor.TransitionAdmission transitionPermit = admission.get();
      ControlPlaneTransitionAdmission tenantAdmission = admissionPolicy.admitTransition(admissionRequest(workItem));
      if (!tenantAdmission.decision().allowed()) {
        transitionPermit.close();
        return handleDeniedAdmission(workItem, tenantAdmission);
      }
      return claimAndRun(workItem, worker, itemContinuationHandler)
          .onTermination().invoke(() -> {
            transitionPermit.close();
            tenantAdmission.permit().close();
          });
    });
  }

  private Uni<Void> handleDeniedAdmission(
      ExecutionWorkItem workItem,
      ControlPlaneTransitionAdmission tenantAdmission) {
    if (ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED
        .equals(tenantAdmission.decision().errorCode())) {
      return workDispatcher.enqueueDelayed(workItem, saturatedDelay.get());
    }
    LOG.warnf(
        "Skipping async execution work item tenant=%s executionId=%s: %s",
        workItem.tenantId(),
        workItem.executionId(),
        tenantAdmission.decision().reason());
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> claimAndRun(
      ExecutionWorkItem workItem,
      PipelineTransitionWorker worker,
      AwaitItemContinuationHandler itemContinuationHandler) {
    long now = System.currentTimeMillis();
    return executionStateStore.claimLease(
            workItem.tenantId(),
            workItem.executionId(),
            queueWorkerId,
            now,
            orchestratorConfig.leaseMs())
        .onItem().transformToUni(claimed -> claimed
            .map(record -> {
              ClaimedSegment segment = ClaimedSegment.from(record);
              return keepLeaseAlive(segment, runClaimed(segment, worker, itemContinuationHandler, now));
            })
            .orElseGet(() -> Uni.createFrom().voidItem()));
  }

  private Uni<Void> keepLeaseAlive(ClaimedSegment segment, Uni<Void> work) {
    long leaseMs = orchestratorConfig.leaseMs();
    Duration renewalInterval = Duration.ofMillis(Math.max(1L, leaseMs / 3L));
    return Uni.join().first(work, renewLeaseUntilLost(segment, renewalInterval)).toTerminate();
  }

  private Uni<Void> renewLeaseUntilLost(ClaimedSegment segment, Duration renewalInterval) {
    return Uni.createFrom().voidItem()
        .onItem().delayIt().by(renewalInterval)
        .onItem().transformToUni(ignored -> {
          ExecutionRecord<Object, Object> record = segment.record();
          long nowEpochMs = System.currentTimeMillis();
          return executionStateStore.renewLease(
                  record.tenantId(),
                  record.executionId(),
                  record.version(),
                  queueWorkerId,
                  nowEpochMs,
                  orchestratorConfig.leaseMs())
              .onItem().transformToUni(renewed -> renewed.isPresent()
                  ? renewLeaseUntilLost(segment, renewalInterval)
                  : Uni.createFrom().failure(new IllegalStateException(
                      "Execution lease ownership was lost for execution " + record.executionId())));
        });
  }

  private Uni<Void> runClaimed(
      ClaimedSegment segment,
      PipelineTransitionWorker worker,
      AwaitItemContinuationHandler itemContinuationHandler,
      long claimedAtEpochMs) {
    ExecutionRecord<Object, Object> record = segment.record();
    LOG.infof(
        "Claimed async execution %s tenant=%s stepIndex=%d attempt=%d resultShape=%s awaitUnitId=%s",
        record.executionId(),
        record.tenantId(),
        record.currentStepIndex(),
        record.attempt(),
        record.resultShape(),
        record.awaitUnitId() == null ? "<none>" : record.awaitUnitId());
    return segmentBoundaryLedger.get()
        .recordSegmentAttemptStarted(record, segment.transitionKey(), claimedAtEpochMs)
        .chain(() -> executeTransition(segment, worker))
        .onItem().transform(result -> SegmentCommitPlan.from(segment, result, payloadCodec.get()))
        .onItem().transformToUni(plan -> segmentCommitEffects.commit(plan, itemContinuationHandler))
        .onFailure(AwaitThrowableSupport::containsAwaitSuspension)
        .recoverWithUni(failure -> suspendedPlan(segment, failure)
            .onItem().transformToUni(plan -> segmentCommitEffects.commit(plan, itemContinuationHandler)))
        .onFailure().recoverWithUni(failure -> segmentCommitEffects.fail(segment, failure));
  }

  private Uni<TransitionResultEnvelope> executeTransition(
      ClaimedSegment segment,
      PipelineTransitionWorker worker) {
    if (isCoordinatorTerminalMaterialization(segment)) {
      // The terminal cursor has no business step left to invoke. Keep its materialized input at
      // the coordinator so SegmentCommitPlan can publish it without serializing a large result
      // through an otherwise no-op remote worker call.
      return Uni.createFrom().item(TransitionResultEnvelope.completedTerminalInputPassthrough());
    }
    return transitionCommand(segment)
        .onItem().transformToUni(command -> transitionWorkerExecutor.execute(worker, command));
  }

  private boolean isCoordinatorTerminalMaterialization(ClaimedSegment segment) {
    return segment.record().resultShape() == ExecutionResultShape.MATERIALIZED_MULTI
        && !segment.resumesFromAwait()
        && segment.record().currentStepIndex() == pipelineStepCount.getAsInt();
  }

  private Uni<TransitionCommandEnvelope> transitionCommand(ClaimedSegment segment) {
    return inputPayload(segment)
        .onItem().transform(payload -> segment.transitionCommand(payload, payloadCodec.get()));
  }

  private Uni<Object> inputPayload(ClaimedSegment segment) {
    if (!segment.resumesFromAwait()) {
      return Uni.createFrom().item(segment.record().inputPayload());
    }
    String awaitUnitId = segment.record().awaitUnitId();
    if (awaitUnitId == null || awaitUnitId.isBlank()) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Execution " + segment.record().executionId() + " is resuming from step "
              + segment.record().currentStepIndex() + " without awaitUnitId"));
    }
    return awaitCoordinator.loadResumePayload(segment.record().tenantId(), awaitUnitId);
  }

  private Uni<SegmentCommitPlan> suspendedPlan(ClaimedSegment segment, Throwable failure) {
    return awaitCoordinator.suspensionSnapshot(AwaitThrowableSupport.extractAwaitSuspension(failure))
        .onItem().transform(suspension -> new SuspendedSegment(segment, suspension));
  }

  private ControlPlaneAdmissionRequest admissionRequest(ExecutionWorkItem workItem) {
    return new ControlPlaneAdmissionRequest(
        workItem.tenantId(),
        ControlPlaneAdmissionOperation.PROCESS_WORK_ITEM,
        null,
        null,
        workItem.executionId(),
        "worker-dispatch",
        true);
  }
}
