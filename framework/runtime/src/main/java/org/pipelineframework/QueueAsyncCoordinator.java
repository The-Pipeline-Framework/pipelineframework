package org.pipelineframework;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import java.time.Duration;
import java.time.Instant;
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
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.NotFoundException;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.awaitable.AwaitThrowableSupport;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.ControlPlaneTransitionAdmission;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.PipelineTelemetry;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRedriveResult;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;
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
    return executePipelineAsync(
        input,
        tenantId,
        idempotencyKey,
        outputStreaming,
        pipelineId(),
        contractVersion(),
        releaseVersion());
  }

  Uni<RunAsyncAcceptedDto> executePipelineAsync(
      Object input,
      String tenantId,
      String idempotencyKey,
      boolean outputStreaming,
      String pipelineId,
      String contractVersion,
      String releaseVersion) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    if (outputStreaming) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode does not support streaming pipeline outputs yet."));
    }
    Object executionInput = executionInputPolicy.normalizeExecutionInput(input);
    RuntimeException inputFailure = executionInputPolicy.validateInputShape(executionInput);
    if (inputFailure != null) {
      return Uni.createFrom().failure(inputFailure);
    }
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    RuntimeException admissionFailure = admissionFailure(admissionRequest(
        resolvedTenant,
        ControlPlaneAdmissionOperation.SUBMIT_EXECUTION,
        pipelineId,
        releaseVersion,
        null,
        "api",
        explicitTenant(tenantId)));
    if (admissionFailure != null) {
      return Uni.createFrom().failure(admissionFailure);
    }
    long now = System.currentTimeMillis();
    long ttlEpochS = Instant.ofEpochMilli(now)
        .plus(Duration.ofDays(Math.max(1, orchestratorConfig.executionTtlDays())))
        .getEpochSecond();

    return executionInputPolicy.resolveExecutionInputPayload(executionInput)
        .onItem().transformToUni(snapshot -> {
              String executionKey;
              try {
                executionKey = scopedRootExecutionKey(
                    pipelineId,
                    releaseVersion,
                    executionInputPolicy.resolveExecutionKey(resolvedTenant, snapshot.payload(), idempotencyKey));
              } catch (IllegalArgumentException e) {
                return Uni.createFrom().failure(new BadRequestException(e.getMessage()));
              }
          ExecutionCreateCommand command = new ExecutionCreateCommand(
              resolvedTenant,
              executionKey,
              pipelineId,
              contractVersion,
              releaseVersion,
              snapshot,
              executionResultShapeResolver.resolve(),
              now,
              ttlEpochS);
          return executionStateStore.createOrGetExecution(command)
              .onItem().transformToUni(created -> {
                Uni<Void> recordSubmitted = created.duplicate()
                    ? Uni.createFrom().voidItem()
                    : segmentBoundaryLedger().recordRunSubmitted(created, command, now);
                Uni<Void> enqueue = created.duplicate()
                    ? Uni.createFrom().voidItem()
                    : workDispatcher.enqueueNow(new ExecutionWorkItem(
                        created.record().tenantId(),
                        created.record().executionId()));
                return recordSubmitted
                    .chain(() -> enqueue)
                    .onItem().transform(ignored -> toRunAccepted(created, now));
              });
        });
  }

  Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    RuntimeException admissionFailure = admissionFailure(admissionRequest(
        resolvedTenant,
        ControlPlaneAdmissionOperation.GET_EXECUTION_STATUS,
        executionId,
        "api",
        explicitTenant(tenantId)));
    if (admissionFailure != null) {
      return Uni.createFrom().failure(admissionFailure);
    }
    return executionStateStore.getExecution(resolvedTenant, executionId)
        .onItem().transform(optional -> optional
            .map(QueueAsyncCoordinator::toStatusDto)
            .orElseThrow(() -> new NotFoundException("Execution not found: " + executionId)));
  }

  @SuppressWarnings("unchecked")
  <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    RuntimeException admissionFailure = admissionFailure(admissionRequest(
        resolvedTenant,
        ControlPlaneAdmissionOperation.GET_EXECUTION_RESULT,
        executionId,
        "api",
        explicitTenant(tenantId)));
    if (admissionFailure != null) {
      return Uni.createFrom().failure(admissionFailure);
    }
    return executionStateStore.getExecution(resolvedTenant, executionId)
        .onItem().transform(optional -> optional.orElseThrow(
            () -> new NotFoundException("Execution not found: " + executionId)))
        .onItem().transform(record -> {
          if (record.status() == ExecutionStatus.SUCCEEDED) {
            if (record.resultPayload() == null) {
              return null;
            }
            List<?> items = (List<?>) record.resultPayload();
            if (record.resultShape() == ExecutionResultShape.SINGLE) {
              if (items.size() > 1) {
                throw new IllegalStateException(
                    "Execution " + executionId + " stored multiple terminal items for SINGLE result shape");
              }
              if (outputStreaming) {
                return (T) List.copyOf(items);
              }
              if (items.isEmpty()) {
                return null;
              }
              return (T) coerceStoredResult(items.getFirst(), outputType);
            }
            if (!outputStreaming) {
              throw new IllegalStateException(
                  "Execution " + executionId + " produced a materialized multi result. Request list retrieval instead.");
            }
            return (T) coerceStoredResults(items, outputType);
          }
          if (record.status().terminal()) {
            throw new IllegalStateException("Execution finished without a successful result: " + record.status());
          }
          throw new IllegalStateException("Execution is not complete yet: " + record.status());
        });
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
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = executionInputPolicy.normalizeTenant(tenantId);
    RuntimeException admissionFailure = admissionFailure(admissionRequest(
        resolvedTenant,
        ControlPlaneAdmissionOperation.GET_EXECUTION_RESULT,
        executionId,
        "api",
        explicitTenant(tenantId)));
    if (admissionFailure != null) {
      return Uni.createFrom().failure(admissionFailure);
    }
    return executionStateStore.getExecution(resolvedTenant, executionId)
        .onItem().transform(optional -> optional.orElseThrow(
            () -> new NotFoundException("Execution not found: " + executionId)))
        .onItem().transform(record -> {
          if (record.status() == ExecutionStatus.SUCCEEDED) {
            if (record.resultPayload() == null) {
              return null;
            }
            List<?> items = (List<?>) record.resultPayload();
            if (record.resultShape() == ExecutionResultShape.SINGLE) {
              if (items.size() > 1) {
                throw new IllegalStateException(
                    "Execution " + executionId + " stored multiple terminal items for SINGLE result shape");
              }
              return items.isEmpty() ? null : items.getFirst();
            }
            return List.copyOf(items);
          }
          if (record.status().terminal()) {
            throw new IllegalStateException("Execution finished without a successful result: " + record.status());
          }
          throw new IllegalStateException("Execution is not complete yet: " + record.status());
        });
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
    Optional<TransitionWorkerExecutor.TransitionAdmission> admission = transitionWorkerExecutor.tryAdmit();
    if (admission.isEmpty()) {
      return workDispatcher.enqueueDelayed(workItem, saturatedDelay());
    }
    TransitionWorkerExecutor.TransitionAdmission permit = admission.get();
    ControlPlaneTransitionAdmission tenantAdmission = admissionPolicy().admitTransition(admissionRequest(
        workItem.tenantId(),
        ControlPlaneAdmissionOperation.PROCESS_WORK_ITEM,
        workItem.executionId(),
        "worker-dispatch",
        true));
    if (!tenantAdmission.decision().allowed()) {
      permit.close();
      if (ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED.equals(tenantAdmission.decision().errorCode())) {
        return workDispatcher.enqueueDelayed(workItem, saturatedDelay());
      }
      LOG.warnf(
          "Skipping async execution work item tenant=%s executionId=%s: %s",
          workItem.tenantId(),
          workItem.executionId(),
          tenantAdmission.decision().reason());
      return Uni.createFrom().voidItem();
    }
    long now = System.currentTimeMillis();
    return executionStateStore.claimLease(
            workItem.tenantId(),
            workItem.executionId(),
            queueWorkerId,
            now,
            orchestratorConfig.leaseMs())
        .onItem().transformToUni(claimed -> {
          if (claimed.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          ExecutionRecord<Object, Object> record = claimed.get();
          LOG.infof(
              "Claimed async execution %s tenant=%s stepIndex=%d attempt=%d resultShape=%s awaitUnitId=%s",
              record.executionId(),
              record.tenantId(),
              record.currentStepIndex(),
              record.attempt(),
              record.resultShape(),
              record.awaitUnitId() == null ? "<none>" : record.awaitUnitId());
          String transitionKey = transitionKey(record.executionId(), record.currentStepIndex(), record.attempt());
          return segmentBoundaryLedger().recordSegmentAttemptStarted(record, transitionKey, now)
              .chain(() -> prepareTransitionCommand(record, transitionKey))
              .onItem().transformToUni(command -> transitionWorkerExecutor.execute(worker, command))
              .onItem().transformToUni(result -> handleTransitionResult(record, transitionKey, result, itemContinuationHandler))
              .onFailure(AwaitThrowableSupport::containsAwaitSuspension).recoverWithUni(failure ->
                  awaitCoordinator.suspensionSnapshot(AwaitThrowableSupport.extractAwaitSuspension(failure))
                      .onItem().transformToUni(suspension ->
                          markWaitingExternal(record, suspension, transitionKey, itemContinuationHandler)))
              .onFailure().recoverWithUni(
                  failure -> executionFailureHandler.handleExecutionFailure(
                      record,
                      transitionKey,
                      failure,
                      executionStateStore,
                      workDispatcher,
                      deadLetterPublisher));
        })
        .onTermination().invoke(() -> {
          permit.close();
          tenantAdmission.permit().close();
        });
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

  private Uni<Void> handleTransitionResult(
      ExecutionRecord<Object, Object> record,
      String transitionKey,
      TransitionResultEnvelope result,
      AwaitItemContinuationHandler itemContinuationHandler) {
    if (result == null) {
      return executionFailureHandler.handleExecutionFailure(
          record,
          transitionKey,
          new IllegalStateException("PipelineTransitionWorker returned null result"),
          executionStateStore,
          workDispatcher,
          deadLetterPublisher);
    }
    if (result.outcome() == TransitionWorkerOutcome.COMPLETED) {
      return handleCompletedTransition(record, transitionKey, result);
    }
    if (result.outcome() == TransitionWorkerOutcome.WAITING_EXTERNAL) {
      return markWaitingExternal(record, result.awaitSuspension(), transitionKey, itemContinuationHandler);
    }
    return executionFailureHandler.handleExecutionFailure(
        record,
        transitionKey,
        result.failure().toException(),
        executionStateStore,
        workDispatcher,
        deadLetterPublisher);
  }

  private Uni<Void> handleCompletedTransition(
      ExecutionRecord<Object, Object> record,
      String transitionKey,
      TransitionResultEnvelope result) {
    List<?> outputItems = result.coordinatorOutputItems();
    if (record.resultShape() == ExecutionResultShape.SINGLE && outputItems.size() > 1) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue execution " + record.executionId()
              + " produced " + outputItems.size()
              + " terminal items for SINGLE result shape"));
    }
    long nowEpochMs = System.currentTimeMillis();
    return segmentBoundaryLedger().recordSegmentCompleted(record, transitionKey, result, nowEpochMs)
        .chain(() -> checkpointPublicationService.publishIfConfigured(record, singleResult(outputItems)))
        .chain(() -> publishTerminalOutputsIfConfigured(record, transitionKey, result, nowEpochMs))
        .replaceWith(outputItems)
        .onItem().transformToUni(payload -> executionStateStore.markSucceeded(
            record.tenantId(),
            record.executionId(),
            record.version(),
            transitionKey,
            payload,
            nowEpochMs))
        .onItem().transformToUni(updated -> updated
            .map(succeeded -> segmentBoundaryLedger().recordRunSucceeded(succeeded, outputItems, nowEpochMs))
            .orElseGet(() -> Uni.createFrom().voidItem()))
        .replaceWithVoid();
  }

  private Uni<Void> publishTerminalOutputsIfConfigured(
      ExecutionRecord<Object, Object> record,
      String transitionKey,
      TransitionResultEnvelope result,
      long nowEpochMs) {
    if (result.terminalOutputPublished()) {
      return segmentBoundaryLedger().recordTerminalPublicationCompleted(record, transitionKey, nowEpochMs);
    }
    if (objectPublishCompletionService == null) {
      return Uni.createFrom().voidItem();
    }
    return objectPublishCompletionService.publishIfConfigured(() -> result.decodeOutputItems(payloadCodec()))
        .chain(() -> segmentBoundaryLedger().recordTerminalPublicationCompleted(record, transitionKey, nowEpochMs));
  }

  private Uni<Void> markWaitingExternal(
      ExecutionRecord<Object, Object> record,
      TransitionAwaitSuspension suspended,
      String transitionKey,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return awaitCoordinator.importSuspension(suspended)
        .onItem().transformToUni(ignored -> {
          long nowEpochMs = System.currentTimeMillis();
          return executionStateStore.markWaitingExternal(
            record.tenantId(),
            record.executionId(),
            record.version(),
            transitionKey,
            suspended.unitId(),
            suspended.stepIndex(),
            nowEpochMs)
              .onItem().transformToUni(updated -> {
                if (updated.isPresent()) {
                  LOG.infof(
                      "Execution %s persisted WAITING_EXTERNAL at stepIndex=%d awaitUnitId=%s awaitInteractions=%d",
                      record.executionId(),
                      suspended.stepIndex(),
                      suspended.unitId(),
                      suspended.interactions().size());
                  recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
                      AwaitReplayLifecycleEvent.EXECUTION_WAITING,
                      record.executionId(),
                      suspended.unitId(),
                      null,
                      suspended.stepIndex(),
                      ExecutionStatus.WAITING_EXTERNAL.name(),
                      null,
                      null,
                      null,
                      null,
                      null,
                      null,
                      null));
                  return segmentBoundaryLedger().recordSegmentSuspended(record, transitionKey, suspended, nowEpochMs)
                      .chain(() -> releaseAlreadyCompletedAwaitUnit(
                          updated.get(),
                          suspended,
                          nowEpochMs,
                          itemContinuationHandler));
                }
                return Uni.createFrom().failure(new IllegalStateException(
                    "Failed to persist WAITING_EXTERNAL state for execution "
                        + record.executionId()
                        + " at step "
                        + suspended.stepIndex()
                        + " (expectedVersion="
                        + record.version()
                        + ", awaitUnitId="
                        + suspended.unitId()
                        + ")"));
              });
        });
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

  private Uni<List<?>> runAsyncExecution(
      ExecutionRecord<Object, Object> record,
      PipelineTransitionWorker worker) {
    String transitionKey = transitionKey(record.executionId(), record.currentStepIndex(), record.attempt());
    return prepareTransitionCommand(record, transitionKey)
        .onItem().transformToUni(command -> transitionWorkerExecutor.execute(worker, command))
        .onItem().transform(result -> {
          if (result.outcome() != TransitionWorkerOutcome.COMPLETED) {
            throw new IllegalStateException("Transition did not complete: " + result.outcome());
          }
          List<?> copied = result.coordinatorOutputItems();
          if (record.resultShape() == ExecutionResultShape.SINGLE && copied.size() > 1) {
            throw new IllegalStateException(
                "Async queue execution " + record.executionId()
                    + " produced " + copied.size()
                    + " terminal items for SINGLE result shape");
          }
          return copied;
        });
  }

  private Uni<TransitionCommandEnvelope> prepareTransitionCommand(
      ExecutionRecord<Object, Object> record,
      String transitionKey) {
    Uni<Object> inputPayload = record.currentStepIndex() > 0 && hasAwaitUnitId(record)
        ? loadAwaitResumePayload(record)
        : Uni.createFrom().item(record.inputPayload());
    return inputPayload.onItem().transform(payload -> {
      TransitionWorkerCommand command = new TransitionWorkerCommand(
          record.tenantId(),
          record.executionId(),
          record.currentStepIndex(),
          record.attempt(),
          record.resultShape(),
          record.version(),
          transitionKey,
          payload);
      return TransitionCommandEnvelope.from(
          command,
          record.pipelineId(),
          record.contractVersion(),
          record.releaseVersion(),
          transitionKey,
          payloadCodec().encode(payload));
    });
  }

  private static boolean hasAwaitUnitId(ExecutionRecord<Object, Object> record) {
    return record.awaitUnitId() != null && !record.awaitUnitId().isBlank();
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

  private static String scopedRootExecutionKey(String pipelineId, String releaseVersion, String executionKey) {
    return compositeScopedKey("pipelineId", pipelineId, "releaseVersion", releaseVersion)
        + ":executionKey:"
        + requireScopedValue("executionKey", executionKey);
  }

  private static String compositeScopedKey(String leftName, String left, String rightName, String right) {
    String safeLeft = requireScopedValue(leftName, left);
    String safeRight = requireScopedValue(rightName, right);
    return safeLeft.length() + ":" + safeLeft + ":" + safeRight.length() + ":" + safeRight;
  }

  private static String requireScopedValue(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private Uni<Object> loadAwaitResumePayload(ExecutionRecord<Object, Object> record) {
    if (record.awaitUnitId() == null || record.awaitUnitId().isBlank()) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Execution " + record.executionId() + " is resuming from step "
              + record.currentStepIndex() + " without awaitUnitId"));
    }
    return awaitCoordinator.loadResumePayload(record.tenantId(), record.awaitUnitId());
  }

  private Duration saturatedDelay() {
    PipelineOrchestratorConfig.WorkerConfig workerConfig = orchestratorConfig.worker();
    if (workerConfig == null || workerConfig.saturatedDelay() == null) {
      return Duration.ofSeconds(1);
    }
    return workerConfig.saturatedDelay();
  }

  private Object singleResult(List<?> items) {
    if (items == null || items.isEmpty()) {
      return null;
    }
    return items.getFirst();
  }

  private Object coerceStoredResult(Object result, Class<?> outputType) {
    if (result instanceof SerializedTransitionPayload serialized) {
      return coerceStoredResult(payloadCodec().decode(serialized), outputType);
    }
    if (result == null || outputType == null || outputType.isInstance(result)) {
      return result;
    }
    try {
      return PipelineJson.mapper().convertValue(result, outputType);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "Failed to coerce stored result from "
              + result.getClass().getName()
              + " to "
              + outputType.getName(),
          e);
    }
  }

  private List<?> coerceStoredResults(List<?> results, Class<?> outputType) {
    if (outputType == null) {
      return List.copyOf(results);
    }
    return results.stream()
        .map(result -> coerceStoredResult(result, outputType))
        .toList();
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

  private Uni<Void> releaseAlreadyCompletedAwaitUnit(
      ExecutionRecord<Object, Object> record,
      TransitionAwaitSuspension suspended,
      long nowEpochMs,
      AwaitItemContinuationHandler itemContinuationHandler) {
    return awaitContinuations().afterParentWaiting(record, suspended, nowEpochMs, itemContinuationHandler);
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

  private static RunAsyncAcceptedDto toRunAccepted(CreateExecutionResult created, long nowEpochMs) {
    String executionId = created.record().executionId();
    return new RunAsyncAcceptedDto(
        executionId,
        created.duplicate(),
        "/pipeline/executions/" + executionId,
        nowEpochMs);
  }

  private static ExecutionStatusDto toStatusDto(ExecutionRecord<Object, Object> record) {
    return new ExecutionStatusDto(
        record.executionId(),
        record.status(),
        record.currentStepIndex(),
        record.attempt(),
        record.version(),
        record.nextDueEpochMs(),
        record.updatedAtEpochMs(),
        record.errorCode(),
        record.errorMessage());
  }

  private static String transitionKey(String executionId, int stepIndex, int attempt) {
    return executionId + ":" + stepIndex + ":" + attempt;
  }

}
