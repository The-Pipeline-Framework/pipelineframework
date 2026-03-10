/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Locale;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.NotFoundException;

import io.grpc.Status;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.multi.AbstractMultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.subscription.Cancellable;
import lombok.Getter;
import org.apache.commons.lang3.time.StopWatch;
import org.jboss.logging.Logger;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;
import org.pipelineframework.config.pipeline.PipelinePlatformResourceLoader;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.telemetry.ApmCompatibilityMetrics;
import org.pipelineframework.telemetry.PipelineTelemetry;
import org.pipelineframework.telemetry.RetryAmplificationGuard;
import org.pipelineframework.telemetry.RetryAmplificationGuardMode;
import org.pipelineframework.telemetry.RpcMetrics;

/**
 * Service responsible for executing pipeline logic.
 * This service provides the shared execution logic that can be used by both
 * the PipelineApplication and the CLI app without duplicating code.
 */
@ApplicationScoped
public class PipelineExecutionService {

  private static final Logger LOG = Logger.getLogger(PipelineExecutionService.class);
  private static final String ORCHESTRATOR_SERVICE = "OrchestratorService";
  private static final String ORCHESTRATOR_METHOD = "Run";

  /** Pipeline configuration for this service. */
  @Inject
  protected PipelineConfig pipelineConfig;

  /** Runner responsible for executing pipeline steps. */
  @Inject
  protected PipelineRunner pipelineRunner;

  /** Health check service to verify dependent services. */
  @Inject
  protected HealthCheckService healthCheckService;

  /** Telemetry helper for guard evaluation. */
  @Inject
  protected PipelineTelemetry telemetry;

  /** Queue mode orchestration configuration. */
  @Inject
  protected PipelineOrchestratorConfig orchestratorConfig;

  /** Available execution state stores. */
  @Inject
  Instance<ExecutionStateStore> executionStateStores;

  /** Available work dispatchers. */
  @Inject
  Instance<WorkDispatcher> workDispatchers;

  /** Available dead-letter publishers. */
  @Inject
  Instance<DeadLetterPublisher> deadLetterPublishers;

  private final ScheduledExecutorService killSwitchExecutor = Executors.newSingleThreadScheduledExecutor(
      runnable -> {
        Thread thread = new Thread(runnable, "tpf-kill-switch");
        thread.setDaemon(true);
        return thread;
      });
  private final ScheduledExecutorService queueSweepExecutor = Executors.newSingleThreadScheduledExecutor(
      runnable -> {
        Thread thread = new Thread(runnable, "tpf-queue-sweeper");
        thread.setDaemon(true);
        return thread;
      });

  private final AtomicReference<StartupHealthState> startupHealthState =
      new AtomicReference<>(StartupHealthState.PENDING);
  private volatile CompletableFuture<Boolean> startupHealthFuture = new CompletableFuture<>();
  @Getter
  private volatile String startupHealthError;
  private volatile ExecutionStateStore executionStateStore;
  private volatile WorkDispatcher workDispatcher;
  private volatile DeadLetterPublisher deadLetterPublisher;
  private volatile ScheduledFuture<?> queueSweepFuture;
  private final String queueWorkerId = "worker-" + UUID.randomUUID();

  /**
   * Startup health check state for dependent services.
   */
  public enum StartupHealthState {
    /** Health checks are running. */
    PENDING,
    /** All dependent services reported healthy. */
    HEALTHY,
    /** One or more services reported unhealthy. */
    UNHEALTHY,
    /** Health checks failed due to an error. */
    ERROR
  }

  /**
   * Default constructor for PipelineExecutionService.
   */
  public PipelineExecutionService() {
  }

  @PostConstruct
  void runStartupHealthChecks() {
    initializeQueueMode();
    List<Object> steps;
    try {
      steps = loadPipelineSteps();
    } catch (PipelineConfigurationException e) {
      LOG.errorf(e, "Pipeline configuration invalid during startup health check: %s", e.getMessage());
      startupHealthError = e.getMessage();
      startupHealthState.set(StartupHealthState.ERROR);
      startupHealthFuture.completeExceptionally(e);
      return;
    } catch (Exception e) {
      LOG.errorf(e, "Unexpected error while loading pipeline steps for health check: %s", e.getMessage());
      startupHealthError = e.getMessage();
      startupHealthState.set(StartupHealthState.ERROR);
      startupHealthFuture.completeExceptionally(e);
      return;
    }

    if (steps == null || steps.isEmpty()) {
      LOG.info("No pipeline steps configured, skipping startup health checks.");
      startupHealthState.set(StartupHealthState.HEALTHY);
      startupHealthFuture.complete(true);
      return;
    }

    CompletableFuture<Boolean> healthCheckFuture = CompletableFuture.supplyAsync(
        () -> healthCheckService.checkHealthOfDependentServices(steps),
            Infrastructure.getDefaultExecutor());
    startupHealthFuture = healthCheckFuture;
    healthCheckFuture.whenComplete((result, throwable) -> {
      if (throwable != null) {
        LOG.errorf(throwable, "Unexpected failure during startup health checks: %s", throwable.getMessage());
        startupHealthState.set(StartupHealthState.ERROR);
        return;
      }
      if (Boolean.TRUE.equals(result)) {
        LOG.info("Startup health checks passed.");
        startupHealthState.set(StartupHealthState.HEALTHY);
      } else {
        LOG.error("Startup health checks failed.");
        startupHealthState.set(StartupHealthState.UNHEALTHY);
      }
    });
  }

  private void initializeQueueMode() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return;
    }
    executionStateStore = selectExecutionStateStore(orchestratorConfig.stateProvider());
    workDispatcher = selectWorkDispatcher(orchestratorConfig.dispatcherProvider());
    deadLetterPublisher = selectDeadLetterPublisher(orchestratorConfig.dlqProvider());

    List<String> providerReadinessErrors = new ArrayList<>();
    executionStateStore.startupValidationError(orchestratorConfig)
        .ifPresent(error -> providerReadinessErrors.add("ExecutionStateStore(" + executionStateStore.providerName() + "): " + error));
    workDispatcher.startupValidationError(orchestratorConfig)
        .ifPresent(error -> providerReadinessErrors.add("WorkDispatcher(" + workDispatcher.providerName() + "): " + error));
    deadLetterPublisher.startupValidationError(orchestratorConfig)
        .ifPresent(error -> providerReadinessErrors.add("DeadLetterPublisher(" + deadLetterPublisher.providerName() + "): " + error));
    if (!providerReadinessErrors.isEmpty()) {
      String readinessMessage = "Queue async provider startup validation failed: " + String.join("; ", providerReadinessErrors);
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

  @PreDestroy
  void shutdownKillSwitchExecutor() {
    killSwitchExecutor.shutdownNow();
    if (queueSweepFuture != null) {
      queueSweepFuture.cancel(false);
    }
    queueSweepExecutor.shutdownNow();
  }

  /**
   * Execute the configured pipeline using the provided input.
   * <p>
   * Relies on the startup health check of dependent services before running the pipeline. If the health check fails,
   * returns a failed Multi with a RuntimeException. If the pipeline runner returns null or an unexpected
   * type, returns a failed Multi with an IllegalStateException. On success returns the Multi produced by
   * the pipeline (a Uni is converted to a Multi) with lifecycle hooks attached for timing and logging.
   *
   * @param input the input Multi supplied to the pipeline steps
   * @return the pipeline result as a Multi; if dependent services are unhealthy the Multi fails with a
   *         RuntimeException, and if the runner returns null or an unexpected type the Multi fails with
   *         an IllegalStateException
   */
  public Multi<?> executePipeline(Multi<?> input) {
    return executePipelineStreaming(input);
  }

  /**
   * Execute the configured pipeline and return a streaming result.
   *
   * <p>Accepts either a {@code Uni} or {@code Multi} input. If the pipeline produces a {@code Uni},
   * it is converted to a {@code Multi}. Health checks and logging hooks mirror those in
   * {@link #executePipeline(Multi)}.</p>
   *
   * @param input the input Uni or Multi supplied to the pipeline steps
   * @param <T> the expected output type
   * @return the pipeline result as a Multi with lifecycle hooks attached
   */
  @SuppressWarnings("unchecked")
  public <T> Multi<T> executePipelineStreaming(Object input) {
    return (Multi<T>) executePipelineStreamingInternal(input);
  }

  /**
   * Execute the configured pipeline and return a unary result.
   *
   * <p>Accepts either a {@code Uni} or {@code Multi} input. If the pipeline produces a stream,
   * the result is a failed {@code Uni} indicating a shape mismatch.</p>
   *
   * @param input the input Uni or Multi supplied to the pipeline steps
   * @param <T> the expected output type
   * @return the pipeline result as a Uni with lifecycle hooks attached
   */
  @SuppressWarnings("unchecked")
  public <T> Uni<T> executePipelineUnary(Object input) {
    return (Uni<T>) executePipelineUnaryInternal(input);
  }

  /**
   * Submits an asynchronous orchestrator execution.
   *
   * @param input execution input payload
   * @param tenantId tenant id from caller context
   * @param idempotencyKey optional caller idempotency key
   * @return accepted response payload
   */
  public Uni<RunAsyncAcceptedDto> executePipelineAsync(Object input, String tenantId, String idempotencyKey) {
    return executePipelineAsync(input, tenantId, idempotencyKey, false);
  }

  /**
   * Submits an asynchronous orchestrator execution.
   *
   * @param input execution input payload
   * @param tenantId tenant id from caller context
   * @param idempotencyKey optional caller idempotency key
   * @param outputStreaming whether the pipeline output is streaming
   * @return accepted response payload
   */
  public Uni<RunAsyncAcceptedDto> executePipelineAsync(
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
    Object executionInput = normalizeExecutionInput(input);
    RuntimeException inputFailure = validateInputShape(executionInput);
    if (inputFailure != null) {
      return Uni.createFrom().failure(inputFailure);
    }
    String resolvedTenant = normalizeTenant(tenantId);
    long now = System.currentTimeMillis();
    long ttlEpochS = Instant.ofEpochMilli(now)
        .plus(Duration.ofDays(Math.max(1, orchestratorConfig.executionTtlDays())))
        .getEpochSecond();
    return resolveExecutionInputPayload(executionInput)
        .onItem().transformToUni(snapshot -> {
          String executionKey;
          try {
            executionKey = resolveExecutionKey(resolvedTenant, snapshot.payload(), idempotencyKey);
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

  /**
   * Reads asynchronous execution status.
   *
   * @param tenantId tenant id from caller context
   * @param executionId execution id
   * @return execution status
   */
  public Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = normalizeTenant(tenantId);
    return executionStateStore.getExecution(resolvedTenant, executionId)
        .onItem().transform(optional -> optional
            .map(PipelineExecutionService::toStatusDto)
            .orElseThrow(() -> new NotFoundException("Execution not found: " + executionId)));
  }

  /**
   * Reads asynchronous execution result.
   *
   * @param tenantId tenant id from caller context
   * @param executionId execution id
   * @param outputType expected output type
   * @param outputStreaming whether the configured pipeline output is streaming
   * @param <T> output type
   * @return execution result payload
   */
  @SuppressWarnings("unchecked")
  public <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    String resolvedTenant = normalizeTenant(tenantId);
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

  /**
   * Handles queue-dispatched work items when using the local event dispatcher.
   *
   * @param workItem execution work item
   */
  void onExecutionWork(@ObservesAsync ExecutionWorkItem workItem) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || workItem == null) {
      return;
    }
    processExecutionWorkItem(workItem)
        .subscribe()
        .with(
            ignored -> { },
            failure -> LOG.errorf(failure, "Failed processing async execution work item %s", workItem));
  }

  /**
   * Processes one execution work item and advances lifecycle state.
   *
   * @param workItem work item
   * @return completion signal
   */
  public Uni<Void> processExecutionWorkItem(ExecutionWorkItem workItem) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
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
          return runAsyncExecution(record.inputPayload())
              .onItem().transformToUni(resultPayload -> executionStateStore.markSucceeded(
                  record.tenantId(),
                  record.executionId(),
                  record.version(),
                  transitionKey,
                  resultPayload,
                  System.currentTimeMillis()))
              .onItem().transformToUni(updated -> {
                if (updated.isPresent()) {
                  return Uni.createFrom().voidItem();
                }
                return Uni.createFrom().voidItem();
              })
              .onFailure().recoverWithUni(failure -> handleExecutionFailure(record, transitionKey, failure));
        });
  }

  /**
   * Returns the current startup health state.
   *
   * @return the current startup health state
   */
  public StartupHealthState getStartupHealthState() {
    return startupHealthState.get();
  }

  /**
   * Block until startup health checks complete, or throw if they fail or time out.
   *
   * @param timeout maximum time to wait for health checks
   * @return the resulting startup health state
   */
  public StartupHealthState awaitStartupHealth(Duration timeout) {
    CompletableFuture<Boolean> future = startupHealthFuture;
    if (future == null) {
      return startupHealthState.get();
    }
    try {
      Boolean result = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (Boolean.TRUE.equals(result) && startupHealthState.get() == StartupHealthState.PENDING) {
        startupHealthState.set(StartupHealthState.HEALTHY);
      }
    } catch (TimeoutException e) {
      throw new RuntimeException("Startup health checks are still running.");
    } catch (Exception e) {
      throw new RuntimeException("Startup health checks failed.", e);
    }
    StartupHealthState state = startupHealthState.get();
    if (state != StartupHealthState.HEALTHY) {
      throw new RuntimeException("Startup health checks failed (" + state + ").");
    }
    return state;
  }

  private Multi<?> executePipelineStreamingInternal(Object input) {
    return Multi.createFrom().deferred(() -> {
      StopWatch watch = new StopWatch();
      List<Object> steps = loadStepsForExecution();
      if (steps == null) {
        return Multi.createFrom().failure(new IllegalStateException("Pipeline steps could not be loaded."));
      }
      RuntimeException healthFailure = healthCheckFailure();
      if (healthFailure != null) {
        return Multi.createFrom().failure(healthFailure);
      }
      RuntimeException inputFailure = validateInputShape(input);
      if (inputFailure != null) {
        return Multi.createFrom().failure(inputFailure);
      }

      Object result = pipelineRunner.run(input, steps);
      return switch (result) {
        case null -> Multi.createFrom().failure(new IllegalStateException("PipelineRunner returned null"));
        case Multi<?> multi -> attachMultiHooks(multi, watch);
        case Uni<?> uni -> attachMultiHooks(uni.toMulti(), watch);
        default -> Multi.createFrom().failure(new IllegalStateException(
            MessageFormat.format("PipelineRunner returned unexpected type: {0}", result.getClass().getName())));
      };
    });
  }

  private Uni<?> executePipelineUnaryInternal(Object input) {
    return Uni.createFrom().deferred(() -> {
      StopWatch watch = new StopWatch();
      List<Object> steps = loadStepsForExecution();
      if (steps == null) {
        return Uni.createFrom().failure(new IllegalStateException("Pipeline steps could not be loaded."));
      }
      RuntimeException healthFailure = healthCheckFailure();
      if (healthFailure != null) {
        return Uni.createFrom().failure(healthFailure);
      }
      RuntimeException inputFailure = validateInputShape(input);
      if (inputFailure != null) {
        return Uni.createFrom().failure(inputFailure);
      }

      Object result = pipelineRunner.run(input, steps);
      return switch (result) {
        case null -> Uni.createFrom().failure(new IllegalStateException("PipelineRunner returned null"));
        case Uni<?> uni -> attachUniHooks(uni, watch);
        case Multi<?> ignored -> Uni.createFrom().failure(new IllegalStateException(
            "PipelineRunner returned stream output where unary output was expected"));
        default -> Uni.createFrom().failure(new IllegalStateException(
            MessageFormat.format("PipelineRunner returned unexpected type: {0}", result.getClass().getName())));
      };
    });
  }

  private List<Object> loadStepsForExecution() {
    try {
      return loadPipelineSteps();
    } catch (PipelineConfigurationException e) {
      LOG.errorf(e, "Failed to load pipeline configuration: %s", e.getMessage());
      return null;
    }
  }

  private RuntimeException healthCheckFailure() {
    StartupHealthState state = startupHealthState.get();
    if (state == StartupHealthState.PENDING) {
      try {
        awaitStartupHealth(Duration.ofMinutes(2));
        return null;
      } catch (RuntimeException e) {
        return e;
      }
    }
    if (state != StartupHealthState.HEALTHY) {
      return new RuntimeException(
          "One or more dependent services are not healthy. Pipeline execution aborted (" + state + ").");
    }
    return null;
  }

  private RuntimeException validateInputShape(Object input) {
    if (input instanceof Uni<?> || input instanceof Multi<?>) {
      return null;
    }
    return new IllegalArgumentException(MessageFormat.format(
        "Pipeline input must be Uni or Multi, got: {0}",
        input == null ? "null" : input.getClass().getName()));
  }

  private static Object normalizeExecutionInput(Object input) {
    if (input instanceof Uni<?> || input instanceof Multi<?>) {
      return input;
    }
    return Uni.createFrom().item(input);
  }

  private static Uni<ExecutionInputSnapshot> resolveExecutionInputPayload(Object input) {
    if (input instanceof Uni<?> uni) {
      return uni.onItem().transform(item -> new ExecutionInputSnapshot(ExecutionInputShape.UNI, item));
    }
    if (input instanceof Multi<?> multi) {
      return multi.collect().asList().onItem().transform(list ->
          new ExecutionInputSnapshot(ExecutionInputShape.MULTI, List.copyOf(list)));
    }
    return Uni.createFrom().item(new ExecutionInputSnapshot(ExecutionInputShape.RAW, input));
  }

  private String normalizeTenant(String tenantId) {
    if (tenantId == null || tenantId.isBlank()) {
      return orchestratorConfig.defaultTenant();
    }
    return tenantId.trim();
  }

  private String resolveExecutionKey(String tenantId, Object input, String clientKey) {
    OrchestratorIdempotencyPolicy policy = orchestratorConfig.idempotencyPolicy();
    String normalizedClientKey = normalizeOptional(clientKey);
    if (policy == OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED) {
      if (normalizedClientKey == null) {
        throw new IllegalArgumentException("Idempotency-Key header is required.");
      }
      return normalizedClientKey;
    }
    if (policy == OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY && normalizedClientKey != null) {
      return normalizedClientKey;
    }
    return deriveServerExecutionKey(tenantId, input);
  }

  private static String normalizeOptional(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }

  private static String deriveServerExecutionKey(String tenantId, Object input) {
    try {
      byte[] payloadBytes = org.pipelineframework.config.pipeline.PipelineJson.mapper().writeValueAsBytes(input);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(tenantId.getBytes(StandardCharsets.UTF_8));
      digest.update((byte) ':');
      digest.update(payloadBytes);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to derive deterministic execution key.", e);
    }
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

  private Uni<List<?>> runAsyncExecution(Object inputPayload) {
    Object reactiveInput = toReplayInput(inputPayload);
    return executePipelineStreaming(reactiveInput)
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

  private Object toReplayInput(Object inputPayload) {
    if (inputPayload instanceof ExecutionInputSnapshot snapshot) {
      if (snapshot.shape() == ExecutionInputShape.MULTI) {
        Object payload = snapshot.payload();
        if (payload == null) {
          return Multi.createFrom().empty();
        }
        if (payload instanceof Iterable<?> iterable) {
          return Multi.createFrom().iterable(iterable);
        }
        return Multi.createFrom().item(payload);
      }
      return Uni.createFrom().item(snapshot.payload());
    }
    // Backward-compatible replay for records persisted before shape metadata.
    if (inputPayload instanceof List<?> list) {
      return Multi.createFrom().iterable(list);
    }
    return Uni.createFrom().item(inputPayload);
  }

  private Uni<Void> handleExecutionFailure(
      ExecutionRecord<Object, Object> record,
      String transitionKey,
      Throwable failure) {
    long now = System.currentTimeMillis();
    int nextAttempt = record.attempt() + 1;
    FailureClassification classification = classifyFailure(failure);
    Throwable classifiedFailure = classification.classifiedThrowable();
    boolean retryableFailure = classification.retryable();
    boolean retryAllowed = nextAttempt <= orchestratorConfig.maxRetries();
    if (retryAllowed && retryableFailure) {
      long nextDue = now + retryDelayMillis(nextAttempt);
          return executionStateStore.scheduleRetry(
                  record.tenantId(),
                  record.executionId(),
                  record.version(),
              nextAttempt,
              nextDue,
              transitionKey,
              classifiedFailure.getClass().getSimpleName(),
                  classifiedFailure.getMessage(),
                  now)
              .onItem().transformToUni(updated -> {
                if (updated.isEmpty()) {
                  return Uni.createFrom().voidItem();
                }
                Duration delay = Duration.ofMillis(Math.max(0L, nextDue - System.currentTimeMillis()));
                return workDispatcher.enqueueDelayed(
                    new ExecutionWorkItem(record.tenantId(), record.executionId()),
                    delay);
          });
    }

    return executionStateStore.markTerminalFailure(
            record.tenantId(),
            record.executionId(),
            record.version(),
            ExecutionStatus.FAILED,
            transitionKey,
            classifiedFailure.getClass().getSimpleName(),
            classifiedFailure.getMessage(),
            now)
        .onItem().transformToUni(updated -> {
          if (updated.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          PipelinePlatformResourceLoader.PlatformMetadata platformMetadata = PipelinePlatformResourceLoader
              .loadPlatform()
              .orElse(null);
          DeadLetterEnvelope envelope = DeadLetterEnvelope.builder()
              .tenantId(record.tenantId())
              .executionId(record.executionId())
              .executionKey(record.executionKey())
              .correlationId(record.executionKey())
              .transitionKey(transitionKey)
              .resourceType("tpf.orchestrator.execution")
              .resourceName(ORCHESTRATOR_SERVICE + "/" + ORCHESTRATOR_METHOD)
              .transport(resolveTransport(platformMetadata))
              .platform(resolvePlatform(platformMetadata))
              .terminalStatus(ExecutionStatus.FAILED.name())
              .terminalReason(retryableFailure ? "retry_exhausted" : "non_retryable")
              .errorCode(classifiedFailure.getClass().getSimpleName())
              .errorMessage(classifiedFailure.getMessage())
              .retryable(retryableFailure)
              .retriesObserved(record.attempt())
              .createdAtEpochMs(now)
              .build();
          return deadLetterPublisher.publish(envelope);
        });
  }

  private static String resolveTransport(PipelinePlatformResourceLoader.PlatformMetadata metadata) {
    String candidate = metadata == null ? System.getProperty("pipeline.transport") : metadata.transport();
    if (candidate == null || candidate.isBlank()) {
      return "UNKNOWN";
    }
    return candidate.trim().toUpperCase(Locale.ROOT);
  }

  private static String resolvePlatform(PipelinePlatformResourceLoader.PlatformMetadata metadata) {
    String candidate = metadata == null ? System.getProperty("pipeline.platform") : metadata.platform();
    if (candidate == null || candidate.isBlank()) {
      return "UNKNOWN";
    }
    return candidate.trim().toUpperCase(Locale.ROOT);
  }

  private static FailureClassification classifyFailure(Throwable failure) {
    if (failure == null) {
      Throwable classified = new IllegalStateException("Unknown failure");
      return new FailureClassification(false, classified);
    }
    Throwable nonRetryable = findThrowable(failure, NonRetryableException.class);
    if (nonRetryable != null) {
      return new FailureClassification(false, nonRetryable);
    }
    return new FailureClassification(true, failure);
  }

  private static Throwable findThrowable(Throwable failure, Class<? extends Throwable> targetType) {
    java.util.ArrayDeque<Throwable> queue = new java.util.ArrayDeque<>();
    java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    queue.add(failure);
    while (!queue.isEmpty()) {
      Throwable current = queue.removeFirst();
      if (!seen.add(current)) {
        continue;
      }
      if (targetType.isInstance(current)) {
        return current;
      }
      Throwable cause = current.getCause();
      if (cause != null && cause != current) {
        queue.add(cause);
      }
    }
    return null;
  }

  private record FailureClassification(boolean retryable, Throwable classifiedThrowable) {
  }

  private long retryDelayMillis(int nextAttempt) {
    long base = Math.max(0L, orchestratorConfig.retryDelay().toMillis());
    double multiplier = Math.max(1.0d, orchestratorConfig.retryMultiplier());
    double calculated = base * Math.pow(multiplier, Math.max(0, nextAttempt - 1));
    return Math.min((long) calculated, TimeUnit.MINUTES.toMillis(30));
  }

  private void sweepDueExecutions() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || executionStateStore == null || workDispatcher == null) {
      return;
    }
    long now = System.currentTimeMillis();
    executionStateStore.findDueExecutions(now, orchestratorConfig.sweepLimit())
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
            ignored -> { },
            failure -> LOG.errorf(failure, "Failed sweeping due async executions"));
  }

  private <T> Multi<T> attachRetryAmplificationGuard(Multi<T> multi) {
    if (telemetry == null || !telemetry.retryAmplificationGuardEnabled()) {
      return multi;
    }
    Duration interval = telemetry.retryAmplificationCheckInterval();
    RetryAmplificationGuardMode mode = telemetry.retryAmplificationMode();
    return multi.plug(upstream -> new RetryAmplificationGuardMulti<>(upstream, interval, mode));
  }

  private <T> Uni<T> attachRetryAmplificationGuard(Uni<T> uni) {
    if (telemetry == null || !telemetry.retryAmplificationGuardEnabled()) {
      return uni;
    }
    Duration interval = telemetry.retryAmplificationCheckInterval();
    RetryAmplificationGuardMode mode = telemetry.retryAmplificationMode();
    AtomicBoolean logged = new AtomicBoolean(false);
    return Uni.createFrom().emitter(emitter -> {
      AtomicReference<Cancellable> cancellableRef = new AtomicReference<>();
      AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();
      Cancellable cancellable = uni.subscribe().with(
          item -> {
            ScheduledFuture<?> scheduled = futureRef.get();
            if (scheduled != null) {
              scheduled.cancel(false);
            }
            emitter.complete(item);
          },
          failure -> {
            ScheduledFuture<?> scheduled = futureRef.get();
            if (scheduled != null) {
              scheduled.cancel(false);
            }
            emitter.fail(failure);
          });
      cancellableRef.set(cancellable);
      ScheduledFuture<?> future = killSwitchExecutor.scheduleAtFixedRate(() -> {
        telemetry.retryAmplificationTrigger().ifPresent(trigger -> {
          if (!logged.compareAndSet(false, true)) {
            return;
          }
          logRetryAmplificationTrigger(trigger, mode);
          if (mode == RetryAmplificationGuardMode.FAIL_FAST) {
            emitter.fail(PipelineKillSwitchException.retryAmplification(trigger));
            Cancellable active = cancellableRef.get();
            if (active != null) {
              active.cancel();
            }
          } else {
            ScheduledFuture<?> scheduled = futureRef.get();
            if (scheduled != null) {
              scheduled.cancel(false);
            }
          }
        });
      }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
      futureRef.set(future);
      emitter.onTermination(() -> {
        ScheduledFuture<?> scheduled = futureRef.get();
        if (scheduled != null) {
          scheduled.cancel(false);
        }
      });
    });
  }

  private final class RetryAmplificationGuardMulti<T> extends AbstractMultiOperator<T, T> {
    private final Duration interval;
    private final RetryAmplificationGuardMode mode;

    private RetryAmplificationGuardMulti(
        Multi<? extends T> upstream,
        Duration interval,
        RetryAmplificationGuardMode mode) {
      super(upstream);
      this.interval = interval;
      this.mode = mode;
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> downstream) {
      RetryAmplificationProcessor<T> processor =
          new RetryAmplificationProcessor<>(downstream, interval, mode);
      upstream().subscribe(processor);
    }
  }

  private final class RetryAmplificationProcessor<T> extends MultiOperatorProcessor<T, T> {
    private final RetryAmplificationGuardMode mode;
    private final AtomicBoolean logged;
    private final ScheduledFuture<?> future;

    private RetryAmplificationProcessor(
        MultiSubscriber<? super T> downstream,
        Duration interval,
        RetryAmplificationGuardMode mode) {
      super(downstream);
      this.mode = mode;
      this.logged = new AtomicBoolean(false);
      this.future = killSwitchExecutor.scheduleAtFixedRate(
          this::checkGuard,
          interval.toMillis(),
          interval.toMillis(),
          TimeUnit.MILLISECONDS);
    }

    @Override
    public void onItem(T item) {
      if (!isDone()) {
        downstream.onItem(item);
      }
    }

    @Override
    public void onFailure(Throwable failure) {
      cancelFuture();
      super.onFailure(failure);
    }

    @Override
    public void onCompletion() {
      cancelFuture();
      super.onCompletion();
    }

    @Override
    public void cancel() {
      cancelFuture();
      super.cancel();
    }

    private void checkGuard() {
      telemetry.retryAmplificationTrigger().ifPresent(trigger -> {
        if (!logged.compareAndSet(false, true)) {
          return;
        }
        logRetryAmplificationTrigger(trigger, mode);
        cancelFuture();
        if (mode == RetryAmplificationGuardMode.FAIL_FAST) {
          if (isDone()) {
            return;
          }
          super.onFailure(PipelineKillSwitchException.retryAmplification(trigger));
        }
      });
    }

    private void cancelFuture() {
      if (future != null) {
        future.cancel(false);
      }
    }
  }

  private void logRetryAmplificationTrigger(
      RetryAmplificationGuard.Trigger trigger,
      RetryAmplificationGuardMode mode) {
    String action = mode == RetryAmplificationGuardMode.FAIL_FAST ? "aborting" : "logging";
    LOG.errorf(
        "Retry amplification guard triggered for step %s (inflight slope %.2f > %.2f, retry rate %.2f) over %s (sustain samples %d); %s pipeline run.",
        trigger.step(),
        trigger.inflightSlope(),
        trigger.inflightSlopeThreshold(),
        trigger.retryRate(),
        trigger.window(),
        trigger.sustainSamples(),
        action);
  }

  private <T> Multi<T> attachMultiHooks(Multi<T> multi, StopWatch watch) {
    Multi<T> guarded = attachRetryAmplificationGuard(multi);
    long[] startTime = new long[1];
    return guarded
        .onSubscription().invoke(ignored -> {
          LOG.info("PIPELINE BEGINS processing");
          startTime[0] = System.nanoTime();
          watch.start();
        })
        .onCompletion().invoke(() -> {
          watch.stop();
          long durationNanos = System.nanoTime() - startTime[0];
          RpcMetrics.recordGrpcServer(ORCHESTRATOR_SERVICE, ORCHESTRATOR_METHOD, Status.OK, durationNanos);
          ApmCompatibilityMetrics.recordOrchestratorSuccess(durationNanos / 1_000_000d);
          LOG.infof("✅ PIPELINE FINISHED processing in %s seconds", watch.getTime(TimeUnit.SECONDS));
        })
        .onFailure().invoke(failure -> {
          watch.stop();
          long durationNanos = System.nanoTime() - startTime[0];
          RpcMetrics.recordGrpcServer(ORCHESTRATOR_SERVICE, ORCHESTRATOR_METHOD, Status.fromThrowable(failure),
              durationNanos);
          ApmCompatibilityMetrics.recordOrchestratorFailure(durationNanos / 1_000_000d);
          LOG.errorf(failure, "❌ PIPELINE FAILED after %s seconds", watch.getTime(TimeUnit.SECONDS));
        });
  }

  private <T> Uni<T> attachUniHooks(Uni<T> uni, StopWatch watch) {
    Uni<T> guarded = attachRetryAmplificationGuard(uni);
    long[] startTime = new long[1];
    return guarded
        .onSubscription().invoke(ignored -> {
          LOG.info("PIPELINE BEGINS processing");
          startTime[0] = System.nanoTime();
          watch.start();
        })
        .onItemOrFailure().invoke((item, failure) -> {
          watch.stop();
          long durationNanos = System.nanoTime() - startTime[0];
          if (failure == null) {
            RpcMetrics.recordGrpcServer(ORCHESTRATOR_SERVICE, ORCHESTRATOR_METHOD, Status.OK, durationNanos);
            ApmCompatibilityMetrics.recordOrchestratorSuccess(durationNanos / 1_000_000d);
            LOG.infof("✅ PIPELINE FINISHED processing in %s seconds", watch.getTime(TimeUnit.SECONDS));
          } else {
            RpcMetrics.recordGrpcServer(ORCHESTRATOR_SERVICE, ORCHESTRATOR_METHOD, Status.fromThrowable(failure),
                durationNanos);
            ApmCompatibilityMetrics.recordOrchestratorFailure(durationNanos / 1_000_000d);
            LOG.errorf(failure, "❌ PIPELINE FAILED after %s seconds", watch.getTime(TimeUnit.SECONDS));
          }
        });
  }

  private static double durationMillis(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000d;
  }

  /**
   * Load configured pipeline steps, instantiate them as CDI-managed beans and return them in execution order.
   *
   * <p>If configuration cannot be read or an error occurs while instantiating steps, an exception is thrown to
   * indicate the failure, except when no steps are configured (empty stepConfigs map), which returns an empty list.
   *
   * @return the instantiated pipeline step objects in execution order, or an empty list if no steps are configured
   * @throws PipelineConfigurationException if there are configuration or instantiation failures
   */
  private List<Object> loadPipelineSteps() {
    try {
      // Use the structured configuration mapping to get all pipeline steps
      java.util.Optional<List<String>> resourceOrder = PipelineOrderResourceLoader.loadOrder();
      if (resourceOrder.isEmpty()) {
        if (PipelineOrderResourceLoader.requiresOrder()) {
          throw new PipelineConfigurationException(
              "Pipeline order metadata not found. Ensure META-INF/pipeline/order.json is generated at build time.");
        }
        return Collections.emptyList();
      }
      List<String> orderedStepNames = resourceOrder.get();
      if (orderedStepNames.isEmpty()) {
        throw new PipelineConfigurationException(
            "Pipeline order metadata is empty. Ensure pipeline.yaml defines steps for order generation.");
      }
      if (LOG.isInfoEnabled()) {
        LOG.infof("Loaded pipeline step order (%d steps): %s", orderedStepNames.size(), orderedStepNames);
      }
      return instantiateStepsInOrder(orderedStepNames);
    } catch (Exception e) {
      LOG.errorf(e, "Failed to load configuration: %s", e.getMessage());
      throw new PipelineConfigurationException("Failed to load pipeline configuration: " + e.getMessage(), e);
    }
  }

  private List<Object> instantiateStepsInOrder(List<String> orderedStepNames) {
    List<Object> steps = new ArrayList<>();
    List<String> failedSteps = new ArrayList<>();
    for (String stepClassName : orderedStepNames) {
      Object step = createStepFromConfig(stepClassName);
      if (step != null) {
        steps.add(step);
      } else {
        failedSteps.add(stepClassName);
      }
    }

    if (!failedSteps.isEmpty()) {
      String message = String.format("Failed to instantiate %d step(s): %s",
        failedSteps.size(), String.join(", ", failedSteps));
      LOG.error(message);
      throw new PipelineConfigurationException(message);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debugf("Loaded %d pipeline steps from generated order metadata", steps.size());
    }
    return steps;
  }


  private List<Object> instantiateStepsFromConfig(
    Map<String, org.pipelineframework.config.PipelineStepConfig.StepConfig> stepConfigs) {
    List<String> orderedStepNames = stepConfigs.keySet().stream()
      .sorted()
      .toList();

    List<Object> steps = new ArrayList<>();
    List<String> failedSteps = new ArrayList<>();
    for (String stepClassName : orderedStepNames) {
      Object step = createStepFromConfig(stepClassName);
      if (step != null) {
        steps.add(step);
      } else {
        failedSteps.add(stepClassName);
      }
    }

    if (!failedSteps.isEmpty()) {
      String message = String.format("Failed to instantiate %d step(s): %s",
        failedSteps.size(), String.join(", ", failedSteps));
      LOG.error(message);
      throw new PipelineConfigurationException(message);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debugf("Loaded %d pipeline steps from application properties", steps.size());
    }
    return steps;
  }

  /**
   * Instantiates a pipeline step class and returns the CDI-managed bean.
   *
   * @param stepClassName the fully qualified class name of the pipeline step
   * @return the CDI-managed instance of the step, or null if instantiation fails
   */
  private Object createStepFromConfig(String stepClassName) {
    try {
      ClassLoader[] candidates = new ClassLoader[] {
        Thread.currentThread().getContextClassLoader(),
        PipelineExecutionService.class.getClassLoader(),
        ClassLoader.getSystemClassLoader()
      };

      Class<?> stepClass = null;
      for (ClassLoader candidate : candidates) {
        if (candidate == null) {
          continue;
        }
        try {
          stepClass = Class.forName(stepClassName, true, candidate);
          break;
        } catch (ClassNotFoundException ignored) {
          // try next loader
        }
      }

      if (stepClass == null) {
        throw new ClassNotFoundException(stepClassName);
      }
      io.quarkus.arc.InstanceHandle<?> handle = io.quarkus.arc.Arc.container().instance(stepClass);
      if (!handle.isAvailable()) {
        int beanCount = io.quarkus.arc.Arc.container().beanManager().getBeans(stepClass).size();
        ClassLoader loader = stepClass.getClassLoader();
        LOG.errorf("No CDI bean available for pipeline step %s (beans=%d, loader=%s)",
            stepClassName, beanCount, loader);
        return null;
      }
      return handle.get();
    } catch (Exception e) {
      LOG.errorf(e, "Failed to instantiate pipeline step: %s, error: %s", stepClassName, e.getMessage());
      return null;
    }
  }

  /**
   * Exception thrown when there are configuration issues related to pipeline setup.
   */
  public static class PipelineConfigurationException extends RuntimeException {
      /**
       * Constructs a new PipelineConfigurationException with the specified detail message.
       *
       * @param message the detail message
       */
      public PipelineConfigurationException(String message) {
          super(message);
      }

      /**
       * Constructs a new PipelineConfigurationException with the specified detail message and cause.
       *
       * @param message the detail message
       * @param cause the cause
       */
      public PipelineConfigurationException(String message, Throwable cause) {
          super(message, cause);
      }
  }
}
