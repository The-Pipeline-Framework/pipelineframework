package org.pipelineframework;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.PipelineTelemetry;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRedriveResult;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;

/**
 * Coordinates queue-mode orchestration lifecycle and provider interactions.
 */
@ApplicationScoped
class QueueAsyncCoordinator {

  private static final Logger LOG = Logger.getLogger(QueueAsyncCoordinator.class);
  private static final AwaitItemContinuationHandler NOOP_ITEM_CONTINUATION_HANDLER =
      AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER;

  @Inject
  PipelineOrchestratorConfig orchestratorConfig;

  @Inject
  Instance<ExecutionStateStore> executionStateStores;

  @Inject
  Instance<WorkDispatcher> workDispatchers;

  @Inject
  Instance<DeadLetterPublisher> deadLetterPublishers;

  @Inject
  ExecutionInputPolicy executionInputPolicy;

  @Inject
  ExecutionFailureHandler executionFailureHandler;

  @Inject
  ExecutionResultShapeResolver executionResultShapeResolver;

  @Inject
  CheckpointPublicationService checkpointPublicationService;

  @Inject
  ObjectPublishCompletionService objectPublishCompletionService;

  @Inject
  AwaitCoordinator awaitCoordinator;

  @Inject
  AwaitLiveCompletionRegistry awaitLiveCompletionRegistry;

  @Inject
  TransitionWorkerExecutor transitionWorkerExecutor;

  @Inject
  TransitionPayloadCodec transitionPayloadCodec;

  @Inject
  PipelineReleaseIdentityResolver releaseIdentityResolver;

  @Inject
  ControlPlaneAdmissionPolicy controlPlaneAdmissionPolicy;

  @Inject
  SegmentBoundaryLedger segmentBoundaryLedger;

  private volatile TransitionPayloadCodec fallbackTransitionPayloadCodec;
  private volatile PipelineReleaseIdentityResolver fallbackReleaseIdentityResolver;
  private volatile ControlPlaneAdmissionPolicy fallbackAdmissionPolicy;
  private volatile AwaitContinuations awaitContinuations;
  private volatile ExecutionReadModel executionReadModel;
  private volatile QueueAsyncSubmissionFlow submissionFlow;

  @Inject
  PipelineTelemetry telemetry;

  private final ScheduledExecutorService queueSweepExecutor = Executors.newSingleThreadScheduledExecutor(
      runnable -> {
        Thread thread = new Thread(runnable, "tpf-queue-sweeper");
        thread.setDaemon(true);
        return thread;
      });

  private volatile ScheduledFuture<?> queueSweepFuture;
  volatile ExecutionStateStore executionStateStore;
  volatile WorkDispatcher workDispatcher;
  volatile DeadLetterPublisher deadLetterPublisher;
  private final String queueWorkerId = "worker-" + UUID.randomUUID();

  private volatile boolean queueModeInitialized;

  synchronized void initializeQueueMode() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return;
    }
    if (queueModeInitialized) {
      return;
    }
    executionStateStore = selectExecutionStateStore(orchestratorConfig.stateProvider());
    workDispatcher = selectWorkDispatcher(orchestratorConfig.dispatcherProvider());
    deadLetterPublisher = selectDeadLetterPublisher(orchestratorConfig.dlqProvider());
    executionReadModel = null;
    submissionFlow = null;

    List<String> providerReadinessErrors = new ArrayList<>();
    executionStateStore.startupValidationError(orchestratorConfig)
        .ifPresent(error -> providerReadinessErrors
            .add("ExecutionStateStore(" + executionStateStore.providerName() + "): " + error));
    workDispatcher.startupValidationError(orchestratorConfig)
        .ifPresent(error -> providerReadinessErrors
            .add("WorkDispatcher(" + workDispatcher.providerName() + "): " + error));
    deadLetterPublisher.startupValidationError(orchestratorConfig)
        .ifPresent(error -> providerReadinessErrors
            .add("DeadLetterPublisher(" + deadLetterPublisher.providerName() + "): " + error));
    if (!providerReadinessErrors.isEmpty()) {
      String readinessMessage = "Queue async provider startup validation failed: " + String.join("; ",
          providerReadinessErrors);
      if (orchestratorConfig.strictStartup()) {
        throw new IllegalStateException(readinessMessage);
      }
      LOG.warn(readinessMessage);
    }

    if (orchestratorConfig.strictStartup()
        && orchestratorConfig.idempotencyPolicy() == OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY) {
      throw new IllegalStateException(
          "pipeline.orchestrator.idempotency-policy must be explicitly configured for queue mode when strict startup is enabled.");
    }
    Duration interval = orchestratorConfig.sweepInterval();
    long intervalMs = Math.max(1000L, interval == null ? 30000L : interval.toMillis());
    queueSweepFuture = queueSweepExecutor.scheduleAtFixedRate(
        this::sweepDueExecutions,
        intervalMs,
        intervalMs,
        TimeUnit.MILLISECONDS);
    queueModeInitialized = true;
    LOG.infof("Queue async mode enabled: stateProvider=%s dispatcherProvider=%s dlqProvider=%s",
        executionStateStore.providerName(),
        workDispatcher.providerName(),
        deadLetterPublisher.providerName());
  }

  @PreDestroy
  void shutdownQueueSweepExecutor() {
    if (queueSweepFuture != null) {
      queueSweepFuture.cancel(false);
    }
    queueSweepExecutor.shutdownNow();
  }

  Uni<RunAsyncAcceptedDto> executePipelineAsync(
      Object input,
      String tenantId,
      String idempotencyKey,
      boolean outputStreaming) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return submissionFlow().submit(input, tenantId, idempotencyKey, outputStreaming);
  }

  Uni<RunAsyncAcceptedDto> executePipelineAsync(
      Object input,
      String tenantId,
      String idempotencyKey,
      boolean outputStreaming,
      String pipelineId,
      String contractVersion,
      String releaseVersion) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return submissionFlow().submit(
        input,
        tenantId,
        idempotencyKey,
        outputStreaming,
        pipelineId,
        contractVersion,
        releaseVersion);
  }

  Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return executionReadModel().getExecutionStatus(tenantId, executionId);
  }

  <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return executionReadModel().getExecutionResult(tenantId, executionId, outputType, outputStreaming);
  }

  Uni<ExecutionRedriveResult> redriveExecution(
      String tenantId,
      String executionId,
      Long expectedVersion,
      boolean allowFailed,
      String reason) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    RuntimeException admissionFailure = admissionFailure(admissionRequest(
        resolvedTenant,
        ControlPlaneAdmissionOperation.REDRIVE_EXECUTION,
        executionId,
        "api",
        explicitTenant(tenantId)));
    if (admissionFailure != null) {
      return Uni.createFrom().failure(admissionFailure);
    }
    long now = System.currentTimeMillis();
    return executionStateStore.getExecution(resolvedTenant, executionId)
        .onItem().transformToUni(optional -> {
          if (optional.isEmpty()) {
            return Uni.createFrom().failure(new NotFoundException("Execution not found: " + executionId));
          }
          ExecutionRecord<Object, Object> previous = optional.get();
          if (!redrivable(previous.status(), allowFailed)) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Execution " + executionId + " cannot be re-driven from status " + previous.status()));
          }
          long version = expectedVersion == null ? previous.version() : expectedVersion;
          if (version != previous.version()) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Execution " + executionId + " version mismatch: expected " + version
                    + " but current version is " + previous.version()));
          }
          String transitionKey = redriveTransitionKey(previous, reason);
          return executionStateStore.redriveTerminalExecution(
                  resolvedTenant,
                  executionId,
                  version,
                  allowFailed,
                  transitionKey,
                  now)
              .onItem().transformToUni(redriven -> {
                if (redriven.isEmpty()) {
                  return Uni.createFrom().failure(new IllegalStateException(
                      "Execution " + executionId + " changed before re-drive could be admitted"));
                }
                ExecutionRecord<Object, Object> record = redriven.get();
                return workDispatcher.enqueueNow(new ExecutionWorkItem(record.tenantId(), record.executionId()))
                    .onItem().transform(ignored -> {
                      LOG.infof(
                          "Re-drove execution tenant=%s executionId=%s previousStatus=%s stepIndex=%d attempt=%d reason=%s",
                          record.tenantId(),
                          record.executionId(),
                          previous.status(),
                          record.currentStepIndex(),
                          record.attempt(),
                          normalizeReason(reason));
                      return ExecutionRedriveResult.from(previous, record);
                    });
              });
        });
  }

  Uni<Object> getExecutionResultPayload(String tenantId, String executionId) {
    if (!ensureQueueModeReady()) {
      return Uni.createFrom().failure(queueModeDisabledException());
    }
    return executionReadModel().getExecutionResultPayload(tenantId, executionId);
  }

  Uni<Void> processExecutionWorkItem(ExecutionWorkItem workItem, PipelineTransitionWorker worker) {
    return processExecutionWorkItem(workItem, worker, NOOP_ITEM_CONTINUATION_HANDLER);
  }

  Uni<Void> processExecutionWorkItem(
      ExecutionWorkItem workItem,
      PipelineTransitionWorker worker,
      AwaitItemContinuationHandler itemContinuationHandler) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || workItem == null) {
      return Uni.createFrom().voidItem();
    }
    if (worker == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("PipelineTransitionWorker must not be null"));
    }
    return segmentPipeline().process(workItem, worker, itemContinuationHandler);
  }

  void sweepDueExecutions() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || executionStateStore == null || workDispatcher == null) {
      return;
    }
    long now = System.currentTimeMillis();
    sweepTimedOutAwaitInteractions(now)
        .onItem().transformToUni(ignored -> executionStateStore.findDueExecutions(now, orchestratorConfig.sweepLimit()))
        .onItem().transformToUni(due -> {
          if (due.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          List<Uni<Void>> enqueueOperations = new ArrayList<>(due.size());
          for (ExecutionRecord<Object, Object> record : due) {
            enqueueOperations.add(workDispatcher.enqueueNow(new ExecutionWorkItem(record.tenantId(), record.executionId()))
                .onFailure().transform(failure -> new IllegalStateException(
                    "Failed to re-dispatch due execution " + record.executionId(),
                    failure)));
          }
          return Uni.join().all(enqueueOperations).andCollectFailures().replaceWithVoid();
        })
        .subscribe()
        .with(
            ignored -> {
            },
            failure -> LOG.errorf(failure, "Failed sweeping due async executions"));
  }

  Uni<AwaitCompletionResult> completeAwait(AwaitCompletionCommand command) {
    return completeAwait(command, NOOP_ITEM_CONTINUATION_HANDLER);
  }

  Uni<AwaitCompletionResult> completeAwait(
      AwaitCompletionCommand command,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return awaitBoundaryAdmission().complete(command, itemContinuationHandler);
  }

  Uni<List<AwaitInteractionRecord>> queryPendingAwaitInteractions(
      String tenantId,
      String assignee,
      String group,
      String stepId,
      int limit) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    RuntimeException admissionFailure = admissionFailure(admissionRequest(
        resolvedTenant,
        ControlPlaneAdmissionOperation.QUERY_PENDING_AWAIT,
        null,
        "api",
        explicitTenant(tenantId)));
    if (admissionFailure != null) {
      return Uni.createFrom().failure(admissionFailure);
    }
    return awaitCoordinator.queryPending(resolvedTenant, assignee, group, stepId, limit <= 0 ? 100 : limit);
  }

  private Uni<Void> sweepTimedOutAwaitInteractions(long now) {
    return awaitCoordinator.findTimedOut(now, orchestratorConfig.sweepLimit())
        .onItem().transformToUni(records -> {
          if (records.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          List<Uni<Void>> operations = new ArrayList<>(records.size());
          for (AwaitInteractionRecord record : records) {
            operations.add(awaitCoordinator.markTimedOut(record, now)
                .onItem().transformToUni(updated -> updated.isPresent()
                    ? segmentBoundaryLedger().recordInteractionTimedOut(updated.get(), now)
                    : Uni.createFrom().voidItem())
                .onItem().transformToUni(ignored -> executionStateStore.getExecution(record.tenantId(), record.executionId()))
                .onItem().transformToUni(execution -> {
                  if (execution.isEmpty() || execution.get().status().terminal()) {
                    return Uni.createFrom().voidItem();
                  }
                  ExecutionRecord<Object, Object> executionRecord = execution.get();
                  if (executionRecord.status() != ExecutionStatus.WAITING_EXTERNAL
                      || !record.unitId().equals(executionRecord.awaitUnitId())) {
                    return Uni.createFrom().voidItem();
                  }
                  return executionStateStore.markTerminalFailure(
                          executionRecord.tenantId(),
                          executionRecord.executionId(),
                          executionRecord.version(),
                          ExecutionStatus.FAILED,
                          transitionKey(executionRecord.executionId(), executionRecord.currentStepIndex(), executionRecord.attempt()),
                          "AWAIT_TIMEOUT",
                          "Await interaction timed out: " + record.interactionId(),
                          now)
                      .onItem().transformToUni(updated -> updated
                          .map(failed -> segmentBoundaryLedger().recordRunFailed(
                              failed,
                              "AWAIT_TIMEOUT",
                              "Await interaction timed out: " + record.interactionId(),
                              now))
                          .orElseGet(() -> Uni.createFrom().voidItem()))
                      .replaceWithVoid();
                }));
          }
          return Uni.join().all(operations).andCollectFailures().replaceWithVoid();
        });
  }

  private static boolean redrivable(ExecutionStatus status, boolean allowFailed) {
    return status == ExecutionStatus.DLQ || (allowFailed && status == ExecutionStatus.FAILED);
  }

  private static String redriveTransitionKey(ExecutionRecord<Object, Object> record, String reason) {
    return "redrive:" + record.executionId() + ":" + record.version() + ":" + normalizeReason(reason);
  }

  private static String normalizeReason(String reason) {
    if (reason == null || reason.isBlank()) {
      return "operator";
    }
    String trimmed = reason.trim();
    return trimmed.length() <= 80 ? trimmed : trimmed.substring(0, 80);
  }

  private Duration saturatedDelay() {
    PipelineOrchestratorConfig.WorkerConfig workerConfig = orchestratorConfig.worker();
    if (workerConfig == null || workerConfig.saturatedDelay() == null) {
      return Duration.ofSeconds(1);
    }
    return workerConfig.saturatedDelay();
  }

  private TransitionPayloadCodec payloadCodec() {
    if (transitionPayloadCodec != null) {
      return transitionPayloadCodec;
    }
    TransitionPayloadCodec fallback = fallbackTransitionPayloadCodec;
    if (fallback == null) {
      synchronized (this) {
        fallback = fallbackTransitionPayloadCodec;
        if (fallback == null) {
          fallback = new JsonTransitionPayloadCodec();
          fallbackTransitionPayloadCodec = fallback;
        }
      }
    }
    return fallback;
  }

  private String pipelineId() {
    return releaseIdentityResolver().pipelineId(orchestratorConfig);
  }

  private String contractVersion() {
    return releaseIdentityResolver().contractVersion();
  }

  private String releaseVersion() {
    return releaseIdentityResolver().releaseVersion(orchestratorConfig);
  }

  private PipelineReleaseIdentityResolver releaseIdentityResolver() {
    if (releaseIdentityResolver != null) {
      return releaseIdentityResolver;
    }
    PipelineReleaseIdentityResolver fallback = fallbackReleaseIdentityResolver;
    if (fallback == null) {
      synchronized (this) {
        fallback = fallbackReleaseIdentityResolver;
        if (fallback == null) {
          fallback = new PipelineReleaseIdentityResolver();
          fallbackReleaseIdentityResolver = fallback;
        }
      }
    }
    return fallback;
  }

  private ControlPlaneAdmissionPolicy admissionPolicy() {
    if (controlPlaneAdmissionPolicy != null) {
      return controlPlaneAdmissionPolicy;
    }
    ControlPlaneAdmissionPolicy fallback = fallbackAdmissionPolicy;
    if (fallback == null) {
      synchronized (this) {
        fallback = fallbackAdmissionPolicy;
        if (fallback == null) {
          fallback = new org.pipelineframework.orchestrator.LocalControlPlaneAdmissionPolicy(orchestratorConfig);
          fallbackAdmissionPolicy = fallback;
        }
      }
    }
    return fallback;
  }

  private RuntimeException admissionFailure(ControlPlaneAdmissionRequest request) {
    ControlPlaneAdmissionDecision decision = admissionPolicy().admit(request);
    return decision.allowed() ? null : new ControlPlaneAdmissionException(decision);
  }

  private ControlPlaneAdmissionRequest admissionRequest(
      String tenantId,
      ControlPlaneAdmissionOperation operation,
      String executionId,
      String source,
      boolean explicitTenant) {
    return new ControlPlaneAdmissionRequest(
        tenantId,
        operation,
        pipelineId(),
        releaseVersion(),
        executionId,
        source,
        explicitTenant);
  }

  private ControlPlaneAdmissionRequest admissionRequest(
      String tenantId,
      ControlPlaneAdmissionOperation operation,
      String pipelineId,
      String releaseVersion,
      String executionId,
      String source,
      boolean explicitTenant) {
    return new ControlPlaneAdmissionRequest(
        tenantId,
        operation,
        pipelineId,
        releaseVersion,
        executionId,
        source,
        explicitTenant);
  }

  private static boolean explicitTenant(String tenantId) {
    return tenantId != null && !tenantId.isBlank();
  }

  private boolean ensureQueueModeReady() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return false;
    }
    if (!queueModeInitialized && missingQueueProviders()) {
      initializeQueueMode();
    }
    return true;
  }

  private boolean missingQueueProviders() {
    return executionStateStore == null || workDispatcher == null || deadLetterPublisher == null;
  }

  private static IllegalStateException queueModeDisabledException() {
    return new IllegalStateException(
        "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC.");
  }

  Uni<Void> recordAwaitItemContinuation(
      AwaitInteractionRecord interaction,
      org.pipelineframework.awaitable.AwaitUnitRecord unit,
      int aggregateStepIndex,
      ExecutionInputSnapshot continuationInput,
      List<?> segmentOutputs,
      long nowEpochMs) {
    return awaitContinuations().captureItemContinuationOutput(
        interaction,
        unit,
        aggregateStepIndex,
        continuationInput,
        segmentOutputs,
        nowEpochMs);
  }

  Uni<Void> releaseItemizedAwaitParentIfReady(
      ExecutionRecord<Object, Object> parent,
      org.pipelineframework.awaitable.AwaitUnitRecord unit,
      int aggregateStepIndex,
      long nowEpochMs) {
    return awaitContinuations().releaseParentIfReady(parent, unit, aggregateStepIndex, nowEpochMs);
  }

  private AwaitBoundaryAdmission awaitBoundaryAdmission() {
    return new AwaitBoundaryAdmission(
        orchestratorConfig,
        executionInputPolicy,
        admissionPolicy(),
        awaitCoordinator,
        awaitLiveCompletionRegistry,
        this::pipelineId,
        this::releaseVersion,
        this::segmentBoundaryLedger,
        awaitContinuations());
  }

  private ExecutionReadModel executionReadModel() {
    if (!ensureQueueModeReady()) {
      throw queueModeDisabledException();
    }
    ExecutionReadModel current = executionReadModel;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = executionReadModel;
      if (current == null) {
        current = new ExecutionReadModel(
            orchestratorConfig,
            executionInputPolicy,
            executionStateStore,
            admissionPolicy(),
            this::pipelineId,
            this::releaseVersion,
            this::payloadCodec);
        executionReadModel = current;
      }
      return current;
    }
  }

  private QueueAsyncSubmissionFlow submissionFlow() {
    if (!ensureQueueModeReady()) {
      throw queueModeDisabledException();
    }
    QueueAsyncSubmissionFlow current = submissionFlow;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = submissionFlow;
      if (current == null) {
        current = new QueueAsyncSubmissionFlow(
            orchestratorConfig,
            executionInputPolicy,
            executionResultShapeResolver,
            executionStateStore,
            workDispatcher,
            admissionPolicy(),
            this::pipelineId,
            this::contractVersion,
            this::releaseVersion,
            this::segmentBoundaryLedger);
        submissionFlow = current;
      }
      return current;
    }
  }

  private AwaitContinuations awaitContinuations() {
    AwaitContinuations current = awaitContinuations;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = awaitContinuations;
      if (current == null) {
        current = new AwaitContinuations(
            executionStateStore,
            workDispatcher,
            awaitCoordinator,
            transitionWorkerExecutor,
            queueSweepExecutor,
            this::saturatedDelay,
            this::segmentBoundaryLedger,
            this::recordAwaitLifecycle);
        awaitContinuations = current;
      }
      return current;
    }
  }

  private QueueAsyncSegmentPipeline segmentPipeline() {
    return new QueueAsyncSegmentPipeline(
        orchestratorConfig,
        executionStateStore,
        workDispatcher,
        awaitCoordinator,
        transitionWorkerExecutor,
        admissionPolicy(),
        this::payloadCodec,
        this::segmentBoundaryLedger,
        this::saturatedDelay,
        new SegmentCommitEffects(
            executionStateStore,
            workDispatcher,
            deadLetterPublisher,
            awaitCoordinator,
            executionFailureHandler,
            this::segmentBoundaryLedger,
            awaitContinuations(),
            new TerminalPublicationBoundary(
                checkpointPublicationService,
                objectPublishCompletionService,
                this::payloadCodec,
                this::segmentBoundaryLedger),
            this::recordAwaitLifecycle),
        queueWorkerId);
  }

  private SegmentBoundaryLedger segmentBoundaryLedger() {
    return segmentBoundaryLedger == null ? new SegmentBoundaryLedger() : segmentBoundaryLedger;
  }

  private void recordAwaitLifecycle(AwaitReplayLifecycleEvent lifecycleEvent) {
    if (telemetry != null) {
      try {
        telemetry.recordAwaitLifecycle(lifecycleEvent);
      } catch (Exception e) {
        LOG.warnf(e, "Failed to record await lifecycle event: %s",
            lifecycleEvent == null ? "<null>" : lifecycleEvent.eventName());
      }
    }
  }

  private ExecutionStateStore selectExecutionStateStore(String providerName) {
    return executionStateStores.stream()
        .filter(store -> providerMatches(store.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No ExecutionStateStore provider found for '" + providerName + "'"));
  }

  private WorkDispatcher selectWorkDispatcher(String providerName) {
    return workDispatchers.stream()
        .filter(dispatcher -> providerMatches(dispatcher.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No WorkDispatcher provider found for '" + providerName + "'"));
  }

  private DeadLetterPublisher selectDeadLetterPublisher(String providerName) {
    return deadLetterPublishers.stream()
        .filter(publisher -> providerMatches(publisher.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No DeadLetterPublisher provider found for '" + providerName + "'"));
  }

  private static boolean providerMatches(String availableName, String configuredName) {
    if (configuredName == null || configuredName.isBlank()) {
      return true;
    }
    return configuredName.equalsIgnoreCase(availableName);
  }

  private static String transitionKey(String executionId, int stepIndex, int attempt) {
    return executionId + ":" + stepIndex + ":" + attempt;
  }

}
