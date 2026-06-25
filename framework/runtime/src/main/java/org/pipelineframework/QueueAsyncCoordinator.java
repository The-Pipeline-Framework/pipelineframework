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

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionMetrics;
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
      new AwaitItemContinuationHandler() {
        @Override
        public Uni<Void> continueAwaitItem(
            AwaitInteractionRecord record,
            org.pipelineframework.awaitable.AwaitUnitRecord unit,
            int nextStepIndex,
            Optional<ExecutionRecord<Object, Object>> parent,
            long nowEpochMs) {
          return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> releaseAwaitParentIfReady(
            ExecutionRecord<Object, Object> parent,
            org.pipelineframework.awaitable.AwaitUnitRecord unit,
            int nextStepIndex,
            long nowEpochMs) {
          return Uni.createFrom().voidItem();
        }
      };

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

  private volatile TransitionPayloadCodec fallbackTransitionPayloadCodec;
  private volatile PipelineReleaseIdentityResolver fallbackReleaseIdentityResolver;
  private volatile ControlPlaneAdmissionPolicy fallbackAdmissionPolicy;

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
                Uni<Void> enqueue = created.duplicate()
                    ? Uni.createFrom().voidItem()
                    : workDispatcher.enqueueNow(new ExecutionWorkItem(
                        created.record().tenantId(),
                        created.record().executionId()));
                return enqueue.onItem().transform(ignored -> toRunAccepted(created, now));
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
    return queueAsyncExecutionFlow().processExecutionWorkItem(workItem, worker, itemContinuationHandler);
  }

  void sweepDueExecutions() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || executionStateStore == null || workDispatcher == null) {
      return;
    }
    long now = System.currentTimeMillis();
    sweepTimedOutAwaitInteractions(now)
        .onItem().transformToUni(ignored -> executionStateStore.findDueExecutions(now, orchestratorConfig.sweepLimit()))
        .onItem().transformToMulti(Multi.createFrom()::iterable)
        .onItem().transformToUniAndMerge(record -> captureFailure(
            workDispatcher.enqueueNow(new ExecutionWorkItem(record.tenantId(), record.executionId()))
                .onFailure().transform(failure -> new IllegalStateException(
                    "Failed to re-dispatch due execution " + record.executionId(),
                    failure))))
        .collect().asList()
        .onItem().transformToUni(this::failIfAny)
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
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    AwaitCompletionCommand normalized = new AwaitCompletionCommand(
        executionInputPolicy.normalizeTenant(command.tenantId()),
        command.interactionId(),
        command.correlationId(),
        command.resumeToken(),
        command.idempotencyKey(),
        command.responsePayload(),
        command.actor(),
        command.nowEpochMs());
    RuntimeException admissionFailure = admissionFailure(admissionRequest(
        normalized.tenantId(),
        ControlPlaneAdmissionOperation.COMPLETE_AWAIT,
        null,
        "api",
        explicitTenant(command.tenantId())));
    if (admissionFailure != null) {
      return Uni.createFrom().failure(admissionFailure);
    }
    return awaitCompletionFlow().complete(new QueueAsyncCommand.CompleteAwait(normalized), itemContinuationHandler);
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
    return transitionCommitFlow().commit(record, transitionKey, result, itemContinuationHandler);
  }

  private QueueAsyncExecutionFlow queueAsyncExecutionFlow() {
    return new QueueAsyncExecutionFlow(
        orchestratorConfig,
        executionStateStore,
        workDispatcher,
        transitionWorkerExecutor,
        queueWorkerId,
        new QueueAsyncExecutionFlow.Effects() {
          @Override
          public ControlPlaneTransitionAdmission admitTransition(ExecutionWorkItem workItem) {
            return admissionPolicy().admitTransition(admissionRequest(
                workItem.tenantId(),
                ControlPlaneAdmissionOperation.PROCESS_WORK_ITEM,
                workItem.executionId(),
                "worker-dispatch",
                true));
          }

          @Override
          public Duration saturatedDelay() {
            return QueueAsyncCoordinator.this.saturatedDelay();
          }

          @Override
          public Uni<TransitionCommandEnvelope> prepareTransitionCommand(
              ExecutionRecord<Object, Object> record,
              String transitionKey) {
            return QueueAsyncCoordinator.this.prepareTransitionCommand(record, transitionKey);
          }

          @Override
          public Uni<Void> commitTransition(
              ExecutionRecord<Object, Object> record,
              String transitionKey,
              TransitionResultEnvelope result,
              AwaitItemContinuationHandler itemContinuationHandler) {
            return handleTransitionResult(record, transitionKey, result, itemContinuationHandler);
          }

          @Override
          public Uni<TransitionAwaitSuspension> suspensionSnapshot(Throwable failure) {
            return awaitCoordinator.suspensionSnapshot(
                org.pipelineframework.awaitable.AwaitThrowableSupport.extractAwaitSuspension(failure));
          }

          @Override
          public Uni<Void> markWaitingExternal(
              ExecutionRecord<Object, Object> record,
              String transitionKey,
              TransitionAwaitSuspension suspension,
              AwaitItemContinuationHandler itemContinuationHandler) {
            return transitionCommitFlow().markWaitingExternal(
                new QueueAsyncCommand.TransitionSuspended(record, transitionKey, suspension),
                itemContinuationHandler);
          }

          @Override
          public Uni<Void> handleExecutionFailure(
              ExecutionRecord<Object, Object> record,
              String transitionKey,
              Throwable failure) {
            return executionFailureHandler.handleExecutionFailure(
                record,
                transitionKey,
                failure,
                executionStateStore,
                workDispatcher,
                deadLetterPublisher);
          }

          @Override
          public void recordClaimed(ExecutionRecord<Object, Object> record) {
            LOG.infof(
                "Claimed async execution %s tenant=%s stepIndex=%d attempt=%d resultShape=%s awaitUnitId=%s",
                record.executionId(),
                record.tenantId(),
                record.currentStepIndex(),
                record.attempt(),
                record.resultShape(),
                record.awaitUnitId() == null ? "<none>" : record.awaitUnitId());
          }

          @Override
          public void recordSkipped(ExecutionWorkItem workItem, ControlPlaneTransitionAdmission admission) {
            LOG.warnf(
                "Skipping async execution work item tenant=%s executionId=%s: %s",
                workItem.tenantId(),
                workItem.executionId(),
                admission.decision().reason());
          }
        });
  }

  private AwaitCompletionFlow awaitCompletionFlow() {
    return new AwaitCompletionFlow(
        awaitCoordinator,
        awaitLiveCompletionRegistry,
        new AwaitCompletionFlow.Effects() {
          @Override
          public Uni<Boolean> itemContinuationReady(
              AwaitInteractionRecord record,
              org.pipelineframework.awaitable.AwaitUnitRecord unit) {
            return itemContinuationFlow().itemContinuationReady(record, unit);
          }

          @Override
          public void dispatchItemContinuation(
              AwaitInteractionRecord record,
              org.pipelineframework.awaitable.AwaitUnitRecord unit,
              AwaitItemContinuationHandler itemContinuationHandler,
              long nowEpochMs) {
            itemContinuationFlow().dispatchItemContinuation(record, unit, itemContinuationHandler, nowEpochMs);
          }

          @Override
          public Uni<Void> releaseAwaitResume(
              AwaitInteractionRecord record,
              String awaitUnitId,
              long nowEpochMs) {
            return QueueAsyncCoordinator.this.releaseAwaitResume(record, awaitUnitId, nowEpochMs);
          }

          @Override
          public void recordEarlyCompletionHeld(
              AwaitInteractionRecord record,
              org.pipelineframework.awaitable.AwaitUnitRecord unit) {
            AwaitCompletionMetrics.recordEarlyCompletionHeld(record, unit);
          }
        });
  }

  private TransitionCommitFlow transitionCommitFlow() {
    return new TransitionCommitFlow(
        awaitCoordinator,
        executionStateStore,
        workDispatcher,
        deadLetterPublisher,
        executionFailureHandler,
        terminalPublicationBarrier(),
        new TransitionCommitFlow.Effects() {
          @Override
          public Uni<Void> releaseAlreadyCompletedAwaitUnit(
              ExecutionRecord<Object, Object> record,
              TransitionAwaitSuspension suspended,
              long nowEpochMs,
              AwaitItemContinuationHandler itemContinuationHandler) {
            LOG.infof(
                "Execution %s persisted WAITING_EXTERNAL at stepIndex=%d awaitUnitId=%s awaitInteractions=%d",
                record.executionId(),
                suspended.stepIndex(),
                suspended.unitId(),
                suspended.interactions().size());
            return itemContinuationFlow().releaseAlreadyCompletedAwaitUnit(
                record,
                suspended,
                nowEpochMs,
                itemContinuationHandler);
          }

          @Override
          public void recordExecutionWaiting(AwaitReplayLifecycleEvent lifecycleEvent) {
            recordAwaitLifecycle(lifecycleEvent);
          }
        });
  }

  private TerminalPublicationBarrier terminalPublicationBarrier() {
    return new TerminalPublicationFlow(
        checkpointPublicationService,
        new ObjectPublishTerminalPublisher(objectPublishCompletionService, payloadCodec()));
  }

  private ItemContinuationFlow itemContinuationFlow() {
    return new ItemContinuationFlow(
        awaitCoordinator,
        executionStateStore,
        workDispatcher,
        transitionWorkerExecutor,
        (task, delayMs) -> queueSweepExecutor.schedule(task, delayMs, TimeUnit.MILLISECONDS),
        Infrastructure.getDefaultExecutor(),
        this::saturatedDelay,
        NOOP_ITEM_CONTINUATION_HANDLER,
        (record, unit, suspended, nowEpochMs) -> releaseAwaitResume(
            record.tenantId(),
            record.executionId(),
            unit.unitId(),
            unit.stepId(),
            suspended.stepIndex(),
            null,
            null,
            null,
            null,
            nowEpochMs),
        this::recordAwaitLifecycle);
  }

  private Uni<Void> sweepTimedOutAwaitInteractions(long now) {
    return awaitCoordinator.findTimedOut(now, orchestratorConfig.sweepLimit())
        .onItem().transformToMulti(Multi.createFrom()::iterable)
        .onItem().transformToUniAndMerge(record -> captureFailure(handleTimedOutAwaitInteraction(record, now)))
        .collect().asList()
        .onItem().transformToUni(this::failIfAny);
  }

  private Uni<Void> handleTimedOutAwaitInteraction(AwaitInteractionRecord record, long now) {
    return awaitCoordinator.markTimedOut(record, now)
        .onItem().transformToUni(updated -> updated.isPresent()
            ? executionStateStore.getExecution(record.tenantId(), record.executionId())
                .onItem().transformToUni(execution -> markTimedOutExecutionIfStillWaiting(record, execution, now, true))
            : Uni.createFrom().voidItem());
  }

  private Uni<Void> markTimedOutExecutionIfStillWaiting(
      AwaitInteractionRecord record,
      Optional<ExecutionRecord<Object, Object>> execution,
      long now,
      boolean retryOnLostRace) {
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
        .onItem().transformToUni(updated -> {
          if (updated.isPresent() || !retryOnLostRace) {
            return Uni.createFrom().voidItem();
          }
          return executionStateStore.getExecution(record.tenantId(), record.executionId())
              .onItem().transformToUni(refreshed ->
                  markTimedOutExecutionIfStillWaiting(record, refreshed, now, false));
        });
  }

  private Uni<Optional<Throwable>> captureFailure(Uni<Void> operation) {
    return operation
        .replaceWith(Optional.<Throwable>empty())
        .onFailure().recoverWithItem(Optional::of);
  }

  private Uni<Void> failIfAny(List<Optional<Throwable>> outcomes) {
    List<Throwable> failures = outcomes.stream()
        .flatMap(Optional::stream)
        .toList();
    return failures.isEmpty()
        ? Uni.createFrom().voidItem()
        : Uni.createFrom().failure(new CompositeException(failures.toArray(Throwable[]::new)));
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

  Uni<Void> recordAwaitItemContinuation(
      AwaitInteractionRecord interaction,
      org.pipelineframework.awaitable.AwaitUnitRecord unit,
      int aggregateStepIndex,
      ExecutionInputSnapshot continuationInput,
      List<?> segmentOutputs,
      long nowEpochMs) {
    return itemContinuationFlow().recordAwaitItemContinuation(
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
    return itemContinuationFlow().releaseItemizedAwaitParentIfReady(
        parent,
        unit,
        aggregateStepIndex,
        nowEpochMs);
  }

  private Uni<Void> releaseAwaitResume(
      AwaitInteractionRecord record,
      String awaitUnitId,
      long nowEpochMs) {
    return releaseAwaitResume(
        record.tenantId(),
        record.executionId(),
        awaitUnitId,
        record.stepId(),
        record.stepIndex(),
        record.interactionId(),
        record.correlationId(),
        record.transportType(),
        record.itemIndex(),
        nowEpochMs);
  }

  private Uni<Void> releaseAwaitResume(
      String tenantId,
      String executionId,
      String awaitUnitId,
      String stepId,
      int stepIndex,
      String interactionId,
      String correlationId,
      String transportType,
      Integer itemIndex,
      long nowEpochMs) {
    return executionStateStore.markAwaitCompleted(
            tenantId,
            executionId,
            awaitUnitId,
            stepIndex + 1,
            nowEpochMs)
        .onItem().transformToUni(updated -> {
          if (updated.isPresent()) {
            LOG.infof(
                "Resuming async execution %s from awaitUnitId=%s at nextStepIndex=%d",
                updated.get().executionId(),
                awaitUnitId,
                updated.get().currentStepIndex());
            recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
                AwaitReplayLifecycleEvent.RESUME_RELEASED,
                executionId,
                awaitUnitId,
                stepId,
                stepIndex,
                updated.get().status().name(),
                interactionId,
                correlationId,
                transportType,
                itemIndex,
                null,
                null,
                null));
            AwaitCompletionMetrics.recordResumeReleased(new AwaitReplayLifecycleEvent(
                AwaitReplayLifecycleEvent.RESUME_RELEASED,
                executionId,
                awaitUnitId,
                stepId,
                stepIndex,
                updated.get().status().name(),
                interactionId,
                correlationId,
                transportType,
                itemIndex,
                null,
                null,
                null));
            return workDispatcher.enqueueNow(new ExecutionWorkItem(
                    updated.get().tenantId(),
                    updated.get().executionId()))
                .replaceWithVoid();
          }
          LOG.debugf(
              "Await resume release for execution %s awaitUnitId=%s at stepIndex=%d produced no state update; treating as idempotent no-op",
              executionId,
              awaitUnitId,
              stepIndex);
          return Uni.createFrom().voidItem();
        });
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
