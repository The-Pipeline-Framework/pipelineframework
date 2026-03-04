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
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
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

  /**
   * Initializes queue mode, loads pipeline steps, and performs startup health checks for dependent services.
   *
   * <p>If pipeline configuration is invalid or an unexpected error occurs while loading steps, the startup
   * health state is set to ERROR, the last startup health error message is recorded, and the startup health
   * future is completed exceptionally. If no steps are configured, the startup health state is set to HEALTHY
   * and the startup health future is completed successfully. Otherwise, an asynchronous health check of
   * dependent services is started and the startup health state is set to HEALTHY or UNHEALTHY based on the
   * result; unexpected failures during the asynchronous check set the state to ERROR.</p>
   */
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

  /**
   * Initializes queue-based orchestration when the configured orchestrator mode is QUEUE_ASYNC.
   *
   * If QUEUE_ASYNC is configured, selects and assigns the ExecutionStateStore, WorkDispatcher,
   * and DeadLetterPublisher based on configuration, validates required idempotency policy under
   * strict startup, and schedules a periodic sweep task to re-dispatch due executions.
   *
   * @throws IllegalStateException if strict startup is enabled but no idempotency policy is configured
   */
  private void initializeQueueMode() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return;
    }
    executionStateStore = selectExecutionStateStore(orchestratorConfig.stateProvider());
    workDispatcher = selectWorkDispatcher(orchestratorConfig.dispatcherProvider());
    deadLetterPublisher = selectDeadLetterPublisher(orchestratorConfig.dlqProvider());
    if (orchestratorConfig.strictStartup() && orchestratorConfig.idempotencyPolicy() == null) {
      throw new IllegalStateException("pipeline.orchestrator.idempotency-policy must be configured in queue mode.");
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

  /**
   * Selects the configured ExecutionStateStore provider by name, preferring the highest-priority match.
   *
   * @param providerName the configured provider name to match (null or blank matches any available provider)
   * @return the selected ExecutionStateStore implementation
   * @throws IllegalStateException if no matching provider is available
   */
  private ExecutionStateStore selectExecutionStateStore(String providerName) {
    return executionStateStores.stream()
        .filter(store -> providerMatches(store.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No ExecutionStateStore provider found for '" + providerName + "'"));
  }

  /**
   * Selects the WorkDispatcher implementation that matches the configured provider name and has the highest priority.
   *
   * @param providerName the configured provider name to match (case-insensitive); when null or blank, any provider is considered a match
   * @return the matching WorkDispatcher with the highest priority
   * @throws IllegalStateException if no matching provider is found
   */
  private WorkDispatcher selectWorkDispatcher(String providerName) {
    return workDispatchers.stream()
        .filter(dispatcher -> providerMatches(dispatcher.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No WorkDispatcher provider found for '" + providerName + "'"));
  }

  /**
   * Selects the DeadLetterPublisher implementation that matches the given configured provider name
   * and has the highest priority.
   *
   * @param providerName the configured provider name to match (null or blank matches any provider)
   * @return the matching DeadLetterPublisher implementation with highest priority
   * @throws IllegalStateException if no matching provider is found
   */
  private DeadLetterPublisher selectDeadLetterPublisher(String providerName) {
    return deadLetterPublishers.stream()
        .filter(publisher -> providerMatches(publisher.providerName(), providerName))
        .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "No DeadLetterPublisher provider found for '" + providerName + "'"));
  }

  /**
   * Determines whether an available provider name satisfies the configured provider selection.
   *
   * @param availableName  the provider name offered by an implementation
   * @param configuredName the provider name configured by the user (may be null or blank)
   * @return `true` if `configuredName` is null or blank or equals `availableName` case-insensitively, `false` otherwise
   */
  private static boolean providerMatches(String availableName, String configuredName) {
    if (configuredName == null || configuredName.isBlank()) {
      return true;
    }
    return configuredName.equalsIgnoreCase(availableName);
  }

  /**
   * Shuts down the kill-switch and queue-sweep executors and cancels any scheduled queue sweep task.
   *
   * This method is invoked during bean destruction to stop background schedulers used for retry
   * amplification guard and periodic queue sweeping.
   */
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
     * Execute the configured pipeline and produce a single (unary) result.
     *
     * <p>Accepts either a {@code Uni} or a {@code Multi} as input. If the pipeline produces a stream
     * (a {@code Multi}), this method returns a failed {@code Uni} indicating a shape mismatch.</p>
     *
     * @param input the pipeline input, either a {@code Uni} or a {@code Multi}
     * @param <T> the expected output type
     * @return the pipeline result as a {@code Uni} with lifecycle hooks attached
     */
  @SuppressWarnings("unchecked")
  public <T> Uni<T> executePipelineUnary(Object input) {
    return (Uni<T>) executePipelineUnaryInternal(input);
  }

  /**
   * Accepts and enqueues an execution request for asynchronous (queue) processing.
   *
   * @param input the execution input; may be a Uni or Multi stream or a plain payload (will be normalized)
   * @param tenantId tenant identifier; may be null or blank to use the configured default tenant
   * @param idempotencyKey optional client idempotency key used according to the orchestrator idempotency policy
   * @return a RunAsyncAcceptedDto containing the accepted execution id, whether the request was a duplicate, the resource path, and the accepted epoch
   */
  public Uni<RunAsyncAcceptedDto> executePipelineAsync(Object input, String tenantId, String idempotencyKey) {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
      return Uni.createFrom().failure(new IllegalStateException(
          "Async queue mode is disabled. Set pipeline.orchestrator.mode=QUEUE_ASYNC."));
    }
    Object executionInput = normalizeExecutionInput(input);
    RuntimeException inputFailure = validateInputShape(executionInput);
    if (inputFailure != null) {
      return Uni.createFrom().failure(inputFailure);
    }
    String resolvedTenant = normalizeTenant(tenantId);
    String executionKey;
    try {
      executionKey = resolveExecutionKey(resolvedTenant, input, idempotencyKey);
    } catch (IllegalArgumentException e) {
      return Uni.createFrom().failure(new BadRequestException(e.getMessage()));
    }
    long now = System.currentTimeMillis();
    long ttlEpochS = Instant.ofEpochMilli(now)
        .plus(Duration.ofDays(Math.max(1, orchestratorConfig.executionTtlDays())))
        .getEpochSecond();
    ExecutionCreateCommand command = new ExecutionCreateCommand(
        resolvedTenant,
        executionKey,
        executionInput,
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
  }

  /**
   * Fetches the status details for an asynchronous execution.
   *
   * @param tenantId     tenant id; if null or empty the default tenant from configuration is used
   * @param executionId  identifier of the execution to retrieve
   * @return             an ExecutionStatusDto with execution id, status, current step index, attempt, version, scheduling and error information
   * @throws IllegalStateException if async queue mode is not enabled
   * @throws NotFoundException     if no execution with the given id exists for the resolved tenant
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
   * Retrieve the result payload for a previously scheduled asynchronous execution.
   *
   * @param tenantId the tenant identifier; may be null to use the default tenant
   * @param executionId the execution identifier
   * @param outputType when non-null, the expected concrete type of a non-streaming result item; used for runtime validation
   * @param outputStreaming whether the pipeline's configured output is streaming
   * @param <T> the expected returned payload type
   * @return the execution result: if `outputStreaming` is true, the stored streaming payload; otherwise the first item of the stored result list, or `null` if the list is empty
   * @throws NotFoundException if no execution with the given id exists for the resolved tenant
   * @throws IllegalStateException if the execution is not complete, finished in a terminal failure, or the stored non-streaming result item does not match `outputType`
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
   * Observes asynchronous execution work items and dispatches them for processing when queue-based async orchestration is enabled.
   *
   * @param workItem the execution work item to process; may be ignored if queue async mode is not active
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
   * Process a single queued execution work item and advance the execution's lifecycle based on the outcome.
   *
   * @param workItem the work item containing the tenant and execution identifier to claim and process
   * @return a completion signal: `void` when processing finishes, or a failure if processing could not be completed
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
          ExecutionRecord record = claimed.get();
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
                return Uni.createFrom().failure(new IllegalStateException(
                    "Stale success commit for execution " + record.executionId()));
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

  /**
   * Validates that the provided pipeline input is a Uni or Multi and produces an exception describing the mismatch.
   *
   * @param input the pipeline input to validate; may be null
   * @return `null` if `input` is a `Uni` or `Multi`, otherwise an `IllegalArgumentException` describing the actual input type
   */
  private RuntimeException validateInputShape(Object input) {
    if (input instanceof Uni<?> || input instanceof Multi<?>) {
      return null;
    }
    return new IllegalArgumentException(MessageFormat.format(
        "Pipeline input must be Uni or Multi, got: {0}",
        input == null ? "null" : input.getClass().getName()));
  }

  /**
   * Ensures the execution input is a reactive stream: returns the input unchanged if it's a `Uni` or `Multi`, otherwise wraps the value in a `Uni`.
   *
   * @param input the execution input, which may be a `Uni`, `Multi`, or a plain value
   * @return the original `Uni` or `Multi`, or a `Uni` that emits the provided value
   */
  private static Object normalizeExecutionInput(Object input) {
    if (input instanceof Uni<?> || input instanceof Multi<?>) {
      return input;
    }
    return Uni.createFrom().item(input);
  }

  /**
   * Resolve the tenant identifier, defaulting to the configured tenant when the provided value is null or blank.
   *
   * @param tenantId the tenant identifier provided by the caller; may be null or blank
   * @return the trimmed tenant identifier when present, otherwise the configured default tenant
   */
  private String normalizeTenant(String tenantId) {
    if (tenantId == null || tenantId.isBlank()) {
      return orchestratorConfig.defaultTenant();
    }
    return tenantId.trim();
  }

  /**
   * Resolve the execution idempotency key according to the configured idempotency policy.
   *
   * If the policy requires a client-provided key, the provided `clientKey` is required and returned.
   * If the policy makes the client key optional and one is provided, it is returned; otherwise a server-derived
   * key is computed from `tenantId` and `input`.
   *
   * @param tenantId   the tenant identifier used when deriving a server key
   * @param input      the execution input used when deriving a server key
   * @param clientKey  the optional client-supplied idempotency key (may be null or blank)
   * @return           the resolved execution key to use for idempotency
   * @throws IllegalArgumentException if the configured policy requires a client key but none is provided
   */
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

  /**
   * Normalize a string by trimming whitespace and treating empty or null inputs as absent.
   *
   * @param value the input string, may be null
   * @return the trimmed string, or `null` if the input was null or empty after trimming
   */
  private static String normalizeOptional(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }

  /**
   * Derives a deterministic execution key from the tenant identifier and the JSON-serialized input.
   *
   * @param tenantId the tenant identifier to include as a prefix in the key
   * @param input the execution input object; serialized to JSON for key derivation
   * @return a URL-safe Base64 (no padding) encoding of the SHA-256 digest computed over "tenantId:JSON(input)"
   * @throws IllegalStateException if JSON serialization or digest computation fails
   */
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

  /**
   * Builds a RunAsyncAcceptedDto representing an accepted asynchronous execution.
   *
   * @param created       the result of creating or retrieving the execution; provides the execution id and duplication flag
   * @param nowEpochMs    the current time in epoch milliseconds to record as the acceptance timestamp
   * @return              a DTO containing the execution id, whether the request was a duplicate, a REST path to the execution, and the acceptance epoch ms
   */
  private static RunAsyncAcceptedDto toRunAccepted(CreateExecutionResult created, long nowEpochMs) {
    String executionId = created.record().executionId();
    return new RunAsyncAcceptedDto(
        executionId,
        created.duplicate(),
        "/pipeline/executions/" + executionId,
        nowEpochMs);
  }

  /**
   * Create an ExecutionStatusDto representing the runtime status of an execution.
   *
   * @param record the execution record to convert into a status DTO
   * @return an ExecutionStatusDto containing execution id, status, current step index, attempt,
   *         version, next-due epoch ms, updated-at epoch ms, error code, and error message
   */
  private static ExecutionStatusDto toStatusDto(ExecutionRecord record) {
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

  /**
   * Builds a transition key that identifies a specific execution step attempt.
   *
   * @param executionId the execution's unique identifier
   * @param stepIndex   the step index within the execution
   * @param attempt     the attempt number for the step
   * @return            the transition key in the form "executionId:stepIndex:attempt"
   */
  private static String transitionKey(String executionId, int stepIndex, int attempt) {
    return executionId + ":" + stepIndex + ":" + attempt;
  }

  /**
   * Collects the pipeline's streaming output into a list.
   *
   * @param inputPayload the execution input (may be a `Uni`, a `Multi`, or a raw value that will be normalized)
   * @return a list containing all items emitted by the pipeline execution
   */
  private Uni<List<?>> runAsyncExecution(Object inputPayload) {
    return executePipelineStreaming(inputPayload).collect().asList()
        .onItem().transform(list -> (List<?>) list);
  }

  /**
   * Handles a pipeline execution failure by scheduling a retry if allowed or marking the execution as a terminal failure and publishing a dead-letter envelope.
   *
   * @param record the current execution record
   * @param transitionKey identifier combining execution id, step index, and attempt used to validate state transitions
   * @param failure the throwable that caused the execution failure
   * @return an empty result when failure handling and any follow-up actions (retry enqueue or dead-letter publish) complete
   * @throws IllegalStateException if the state commit is stale and the store reports no update (stale retry or terminal failure commit)
   */
  private Uni<Void> handleExecutionFailure(ExecutionRecord record, String transitionKey, Throwable failure) {
    long now = System.currentTimeMillis();
    int nextAttempt = record.attempt() + 1;
    boolean retryAllowed = nextAttempt <= orchestratorConfig.maxRetries();
    if (retryAllowed) {
      long nextDue = now + retryDelayMillis(nextAttempt);
      return executionStateStore.scheduleRetry(
              record.tenantId(),
              record.executionId(),
              record.version(),
              nextAttempt,
              nextDue,
              transitionKey,
              failure.getClass().getSimpleName(),
              failure.getMessage(),
              now)
          .onItem().transformToUni(updated -> {
            if (updated.isEmpty()) {
              return Uni.createFrom().failure(new IllegalStateException(
                  "Stale retry commit for execution " + record.executionId()));
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
            failure.getClass().getSimpleName(),
            failure.getMessage(),
            now)
        .onItem().transformToUni(updated -> {
          if (updated.isEmpty()) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Stale terminal failure commit for execution " + record.executionId()));
          }
          DeadLetterEnvelope envelope = new DeadLetterEnvelope(
              record.tenantId(),
              record.executionId(),
              transitionKey,
              failure.getClass().getSimpleName(),
              failure.getMessage(),
              now);
          return deadLetterPublisher.publish(envelope);
        });
  }

  /**
   * Compute the retry backoff delay for a given retry attempt using the configured
   * base delay and multiplier, capped at 30 minutes.
   *
   * @param nextAttempt  the retry attempt number (1-based)
   * @return             the delay in milliseconds to wait before the next retry
   */
  private long retryDelayMillis(int nextAttempt) {
    long base = Math.max(0L, orchestratorConfig.retryDelay().toMillis());
    double multiplier = Math.max(1.0d, orchestratorConfig.retryMultiplier());
    double calculated = base * Math.pow(multiplier, Math.max(0, nextAttempt - 1));
    return Math.min((long) calculated, TimeUnit.MINUTES.toMillis(30));
  }

  /**
   * Finds executions due for processing and re-dispatches them to the configured WorkDispatcher.
   *
   * When the orchestrator is not configured for QUEUE_ASYNC or required queue components are missing, this method is a no-op.
   * It queries the ExecutionStateStore for executions whose next-due time is at or before the current time (up to the configured sweep limit)
   * and enqueues a corresponding ExecutionWorkItem for each found execution. Failures to re-dispatch individual executions or to fetch due executions
   * are logged.
   */
  private void sweepDueExecutions() {
    if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC || executionStateStore == null || workDispatcher == null) {
      return;
    }
    long now = System.currentTimeMillis();
    executionStateStore.findDueExecutions(now, orchestratorConfig.sweepLimit())
        .subscribe()
        .with(
            due -> due.forEach(record -> workDispatcher.enqueueNow(
                new ExecutionWorkItem(record.tenantId(), record.executionId()))
                .subscribe().with(ignored -> { }, failure -> LOG.errorf(failure,
                    "Failed to re-dispatch due execution %s", record.executionId()))),
            failure -> LOG.errorf(failure, "Failed sweeping due async executions"));
  }

  /**
   * Wraps the provided Multi with a retry-amplification guard when telemetry indicates the guard is enabled.
   *
   * When telemetry is unavailable or the guard is disabled, the original Multi is returned unchanged.
   *
   * @param multi the upstream Multi to protect
   * @param <T> the stream item type
   * @return a Multi that enforces retry-amplification checks according to telemetry configuration, or the original Multi if the guard is disabled
   */
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
