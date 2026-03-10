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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import lombok.Getter;
import org.apache.commons.lang3.time.StopWatch;
import org.jboss.logging.Logger;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Service responsible for executing pipeline logic.
 * This service provides the shared execution logic that can be used by both
 * the PipelineApplication and the CLI app without duplicating code.
 */
@ApplicationScoped
public class PipelineExecutionService {

  private static final Logger LOG = Logger.getLogger(PipelineExecutionService.class);

  /** Pipeline configuration for this service. */
  @Inject
  protected PipelineConfig pipelineConfig;

  /** Runner responsible for executing pipeline steps. */
  @Inject
  protected PipelineRunner pipelineRunner;

  /** Health check service to verify dependent services. */
  @Inject
  protected HealthCheckService healthCheckService;

  @Inject
  PipelineStepResolver pipelineStepResolver;

  @Inject
  ExecutionHooks executionHooks;

  @Inject
  QueueAsyncCoordinator queueAsyncCoordinator;

  private final java.util.concurrent.atomic.AtomicReference<StartupHealthState> startupHealthState =
      new java.util.concurrent.atomic.AtomicReference<>(StartupHealthState.PENDING);
  private volatile CompletableFuture<Boolean> startupHealthFuture = new CompletableFuture<>();
  @Getter
  private volatile String startupHealthError;

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
    queueAsyncCoordinator.initializeQueueMode();
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
   * Execute the configured pipeline using the provided input.
   */
  public Multi<?> executePipeline(Multi<?> input) {
    return executePipelineStreaming(input);
  }

  /**
   * Execute the configured pipeline and return a streaming result.
   */
  @SuppressWarnings("unchecked")
  public <T> Multi<T> executePipelineStreaming(Object input) {
    return (Multi<T>) executePipelineStreamingInternal(input);
  }

  /**
   * Execute the configured pipeline and return a unary result.
   */
  @SuppressWarnings("unchecked")
  public <T> Uni<T> executePipelineUnary(Object input) {
    return (Uni<T>) executePipelineUnaryInternal(input);
  }

  /**
   * Submits an asynchronous orchestrator execution.
   */
  public Uni<RunAsyncAcceptedDto> executePipelineAsync(Object input, String tenantId, String idempotencyKey) {
    return executePipelineAsync(input, tenantId, idempotencyKey, false);
  }

  /**
   * Submits an asynchronous orchestrator execution.
   */
  public Uni<RunAsyncAcceptedDto> executePipelineAsync(
      Object input,
      String tenantId,
      String idempotencyKey,
      boolean outputStreaming) {
    return queueAsyncCoordinator.executePipelineAsync(input, tenantId, idempotencyKey, outputStreaming);
  }

  /**
   * Reads asynchronous execution status.
   */
  public Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
    return queueAsyncCoordinator.getExecutionStatus(tenantId, executionId);
  }

  /**
   * Reads asynchronous execution result.
   */
  public <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
    return queueAsyncCoordinator.getExecutionResult(tenantId, executionId, outputType, outputStreaming);
  }

  /**
   * Handles queue-dispatched work items when using the local event dispatcher.
   */
  void onExecutionWork(@ObservesAsync ExecutionWorkItem workItem) {
    if (workItem == null) {
      return;
    }
    processExecutionWorkItem(workItem)
        .subscribe()
        .with(
            ignored -> {
            },
            failure -> LOG.errorf(failure, "Failed processing async execution work item %s", workItem));
  }

  /**
   * Processes one execution work item and advances lifecycle state.
   */
  public Uni<Void> processExecutionWorkItem(ExecutionWorkItem workItem) {
    return queueAsyncCoordinator.processExecutionWorkItem(workItem, this::executePipelineStreaming);
  }

  /**
   * Returns the current startup health state.
   */
  public StartupHealthState getStartupHealthState() {
    return startupHealthState.get();
  }

  /**
   * Block until startup health checks complete, or throw if they fail or time out.
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
        case Multi<?> multi -> executionHooks.attachMultiHooks(multi, watch);
        case Uni<?> uni -> executionHooks.attachMultiHooks(uni.toMulti(), watch);
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
        case Uni<?> uni -> executionHooks.attachUniHooks(uni, watch);
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

  RuntimeException validateInputShape(Object input) {
    if (input instanceof Uni<?> || input instanceof Multi<?>) {
      return null;
    }
    return new IllegalArgumentException(MessageFormat.format(
        "Pipeline input must be Uni or Multi, got: {0}",
        input == null ? "null" : input.getClass().getName()));
  }

  List<Object> loadPipelineSteps() {
    return pipelineStepResolver.loadPipelineSteps();
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
