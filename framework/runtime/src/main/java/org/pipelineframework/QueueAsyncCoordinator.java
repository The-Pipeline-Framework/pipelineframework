package org.pipelineframework;

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
import java.util.function.Function;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.NotFoundException;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Coordinates queue-mode orchestration lifecycle and provider interactions.
 */
@ApplicationScoped
class QueueAsyncCoordinator {

  private static final Logger LOG = Logger.getLogger(QueueAsyncCoordinator.class);

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
  CheckpointPublicationService checkpointPublicationService;

  @Inject
  AwaitCoordinator awaitCoordinator;

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

  void initializeQueueMode() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
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
    long now = System.currentTimeMillis();
    long ttlEpochS = Instant.ofEpochMilli(now)
        .plus(Duration.ofDays(Math.max(1, orchestratorConfig.executionTtlDays())))
        .getEpochSecond();

    return executionInputPolicy.resolveExecutionInputPayload(executionInput)
        .onItem().transformToUni(snapshot -> {
          String executionKey;
          try {
            executionKey = executionInputPolicy.resolveExecutionKey(resolvedTenant, snapshot.payload(), idempotencyKey);
          } catch (IllegalArgumentException e) {
            return Uni.createFrom().failure(new BadRequestException(e.getMessage()));
          }
          ExecutionCreateCommand command = new ExecutionCreateCommand(
              resolvedTenant,
              executionKey,
              snapshot,
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
    return executionStateStore.getExecution(resolvedTenant, executionId)
        .onItem().transform(optional -> optional.orElseThrow(
            () -> new NotFoundException("Execution not found: " + executionId)))
        .onItem().transform(record -> {
          if (record.status() == ExecutionStatus.SUCCEEDED) {
            if (record.resultPayload() == null) {
              return null;
            }
            if (outputStreaming) {
              return (T) record.resultPayload();
            }
            List<?> items = (List<?>) record.resultPayload();
            if (items.isEmpty()) {
              return null;
            }
            Object first = items.get(0);
            if (outputType != null && first != null && !outputType.isInstance(first)) {
              throw new IllegalStateException("Stored result type mismatch for execution " + executionId);
            }
            return (T) first;
          }
          if (record.status().terminal()) {
            throw new IllegalStateException("Execution finished without a successful result: " + record.status());
          }
          throw new IllegalStateException("Execution is not complete yet: " + record.status());
        });
  }

  Uni<Void> processExecutionWorkItem(ExecutionWorkItem workItem, Function<ExecutionRecord<Object, Object>, Multi<?>> executeStreaming) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || workItem == null) {
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
          String transitionKey = transitionKey(record.executionId(), record.currentStepIndex(), record.attempt());
          return runAsyncExecution(record, executeStreaming)
              .onItem().transformToUni(resultPayload -> checkpointPublicationService
                  .publishIfConfigured(record, singleResult(resultPayload))
                  .replaceWith(resultPayload))
              .onItem().transformToUni(resultPayload -> executionStateStore.markSucceeded(
                      record.tenantId(),
                      record.executionId(),
                      record.version(),
                      transitionKey,
                      resultPayload,
                      System.currentTimeMillis()))
              .onItem().transformToUni(updated -> Uni.createFrom().voidItem())
              .onFailure(AwaitSuspendedException.class).recoverWithUni(
                  failure -> markWaitingExternal(record, (AwaitSuspendedException) failure, transitionKey))
              .onFailure().recoverWithUni(
                  failure -> executionFailureHandler.handleExecutionFailure(
                      record,
                      transitionKey,
                      failure,
                      executionStateStore,
                      workDispatcher,
                      deadLetterPublisher));
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
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    AwaitCompletionCommand normalized = new AwaitCompletionCommand(
        executionInputPolicy.normalizeTenant(command.tenantId()),
        command.interactionId(),
        command.correlationId(),
        command.idempotencyKey(),
        command.responsePayload(),
        command.actor(),
        command.nowEpochMs());
    return awaitCoordinator.complete(normalized)
        .onFailure().transform(failure -> new IllegalStateException(
            "Failed completing await interaction tenant=" + normalized.tenantId()
                + ", interactionId=" + normalized.interactionId()
                + ", correlationId=" + normalized.correlationId(),
            failure))
        .onItem().transformToUni(result -> validateAwaitCompletionTenant(result, normalized)
            .onItem().transformToUni(validated -> coerceAwaitPayloadAsync(validated.record()))
            .onItem().transformToUni(resumePayload -> executionStateStore.markAwaitCompleted(
                result.record().tenantId(),
                result.record().executionId(),
                result.record().interactionId(),
                resumePayload,
                result.record().stepIndex() + 1,
                normalized.nowEpochMs())
            .onItem().transformToUni(updated -> {
              if (updated.isPresent()) {
                return workDispatcher.enqueueNow(new ExecutionWorkItem(
                        updated.get().tenantId(),
                        updated.get().executionId()))
                    .replaceWith(result);
              }
              return Uni.createFrom().item(result);
            })));
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
    return awaitCoordinator.queryPending(resolvedTenant, assignee, group, stepId, limit <= 0 ? 100 : limit);
  }

  private Uni<Void> markWaitingExternal(
      ExecutionRecord<Object, Object> record,
      AwaitSuspendedException suspended,
      String transitionKey) {
    return executionStateStore.markWaitingExternal(
            record.tenantId(),
            record.executionId(),
            record.version(),
            transitionKey,
            suspended.interactionId(),
            suspended.stepIndex(),
            System.currentTimeMillis())
        .replaceWithVoid();
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
                .onItem().transformToUni(updated -> executionStateStore.getExecution(record.tenantId(), record.executionId()))
                .onItem().transformToUni(execution -> {
                  if (execution.isEmpty() || execution.get().status().terminal()) {
                    return Uni.createFrom().voidItem();
                  }
                  ExecutionRecord<Object, Object> executionRecord = execution.get();
                  if (executionRecord.status() != ExecutionStatus.WAITING_EXTERNAL
                      || !record.interactionId().equals(executionRecord.awaitInteractionId())) {
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
                      .replaceWithVoid();
                }));
          }
          return Uni.join().all(operations).andCollectFailures().replaceWithVoid();
        });
  }

  private Uni<List<?>> runAsyncExecution(
      ExecutionRecord<Object, Object> record,
      Function<ExecutionRecord<Object, Object>, Multi<?>> executeStreaming) {
    return executeStreaming.apply(record)
        .select().first(2)
        .collect().asList()
        .onItem().transformToUni(items -> {
          if (items.size() > 1) {
            return Uni.createFrom().failure(
                new IllegalStateException("Async queue mode does not support streaming pipeline outputs yet."));
          }
          return Uni.createFrom().item((List<?>) List.copyOf(items));
        });
  }

  private Object singleResult(List<?> items) {
    if (items == null || items.isEmpty()) {
      return null;
    }
    return items.getFirst();
  }

  private Object coerceAwaitPayload(AwaitInteractionRecord record) {
    Object payload = record.responsePayload();
    if (payload == null || record.outputType() == null || record.outputType().isBlank()) {
      return payload;
    }
    try {
      Class<?> outputType = Class.forName(record.outputType());
      if (outputType.isInstance(payload)) {
        return payload;
      }
      if (payload instanceof String json) {
        String trimmed = json.trim();
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
          return PipelineJson.mapper().readValue(trimmed, outputType);
        }
      }
      return PipelineJson.mapper().convertValue(payload, outputType);
    } catch (Exception e) {
      throw new IllegalStateException("Failed converting await response payload to " + record.outputType(), e);
    }
  }

  private Uni<Object> coerceAwaitPayloadAsync(AwaitInteractionRecord record) {
    return Uni.createFrom().item(() -> coerceAwaitPayload(record))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
  }

  private Uni<AwaitCompletionResult> validateAwaitCompletionTenant(
      AwaitCompletionResult result,
      AwaitCompletionCommand command) {
    if (!command.tenantId().equals(result.record().tenantId())) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Await completion tenant mismatch: command tenant=" + command.tenantId()
              + ", record tenant=" + result.record().tenantId()));
    }
    return Uni.createFrom().item(result);
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
