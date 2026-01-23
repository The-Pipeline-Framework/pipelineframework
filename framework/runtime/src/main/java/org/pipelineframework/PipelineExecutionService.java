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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.grpc.Status;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import lombok.Getter;
import org.apache.commons.lang3.time.StopWatch;
import org.jboss.logging.Logger;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;
import org.pipelineframework.telemetry.ApmCompatibilityMetrics;
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

  private final AtomicReference<StartupHealthState> startupHealthState =
      new AtomicReference<>(StartupHealthState.PENDING);
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

  private <T> Multi<T> attachMultiHooks(Multi<T> multi, StopWatch watch) {
    long[] startTime = new long[1];
    return multi
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
    long[] startTime = new long[1];
    return uni
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
