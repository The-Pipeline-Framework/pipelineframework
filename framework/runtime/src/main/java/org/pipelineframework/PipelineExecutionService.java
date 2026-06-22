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
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitPayloadSupport;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.awaitable.AwaitThrowableSupport;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.PipelineTransitionWorkerSelector;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.step.Configurable;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.functional.ManyToOne;

/**
 * Service responsible for executing pipeline logic.
 * This service provides the shared execution logic that can be used by both
 * the PipelineApplication and the CLI app without duplicating code.
 */
@ApplicationScoped
public class PipelineExecutionService implements PipelineTransitionWorker {

  private static final Logger LOG = Logger.getLogger(PipelineExecutionService.class);

  /** Pipeline configuration for this service. */
  @Inject
  protected PipelineConfig pipelineConfig;

  /** Runner responsible for executing pipeline steps. */
  @Inject
  protected PipelineRunner pipelineRunner;

  @Inject
  PipelineStepOrderer stepOrderer;

  /** Health check service to verify dependent services. */
  @Inject
  protected HealthCheckService healthCheckService;

  @Inject
  PipelineStepResolver pipelineStepResolver;

  @Inject
  ConfigFactory configFactory;

  @Inject
  ExecutionHooks executionHooks;

  @Inject
  ExecutionInputPolicy executionInputPolicy;

  @Inject
  AwaitCoordinator awaitCoordinator;

  @Inject
  QueueAsyncCoordinator queueAsyncCoordinator;

  @Inject
  PipelineControlPlane controlPlane;

  @Inject
  PipelineOrchestratorConfig orchestratorConfig;

  @Inject
  PipelineTransitionWorkerSelector transitionWorkerSelector;

  @Inject
  TransitionWorkerExecutor transitionWorkerExecutor;

  @Inject
  TransitionPayloadCodec transitionPayloadCodec;

  @Inject
  PipelineReleaseIdentityResolver releaseIdentityResolver;

  private volatile TransitionPayloadCodec fallbackPayloadCodec;
  private volatile PipelineReleaseIdentityResolver fallbackReleaseIdentityResolver;

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
    controlPlane.initializeQueueMode();
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
    return controlPlane.executePipelineAsync(input, tenantId, idempotencyKey, outputStreaming);
  }

  /**
   * Reads asynchronous execution status.
   */
  public Uni<ExecutionStatusDto> getExecutionStatus(String tenantId, String executionId) {
    return controlPlane.getExecutionStatus(tenantId, executionId);
  }

  /**
   * Reads asynchronous execution result.
   */
  public <T> Uni<T> getExecutionResult(String tenantId, String executionId, Class<?> outputType, boolean outputStreaming) {
    return controlPlane.getExecutionResult(tenantId, executionId, outputType, outputStreaming);
  }

  /**
   * Completes a durable await interaction and schedules owning execution continuation.
   */
  public Uni<AwaitCompletionResult> completeAwaitInteraction(AwaitCompletionCommand command) {
    PipelineTransitionWorker selectedWorker = transitionWorkerSelector.select(this);
    return controlPlane.completeAwait(command, awaitItemContinuationHandler(selectedWorker));
  }

  private AwaitItemContinuationHandler awaitItemContinuationHandler(PipelineTransitionWorker selectedWorker) {
    return new AwaitItemContinuationHandler() {
      @Override
      public Uni<Void> continueAwaitItem(
          AwaitInteractionRecord record,
          AwaitUnitRecord unit,
          int nextStepIndex,
          java.util.Optional<ExecutionRecord<Object, Object>> parent,
          long nowEpochMs) {
        if (parent.isEmpty()) {
          return Uni.createFrom().voidItem();
        }
        List<Object> steps = loadStepsForExecution();
        List<Object> orderedSteps = stepOrderer.orderSteps(steps);
        int aggregateStepIndex = firstAggregateStepIndex(orderedSteps, nextStepIndex);
        Object awaitPayload = coerceAwaitItemPayload(record);
        ExecutionInputSnapshot continuationInput = new ExecutionInputSnapshot(ExecutionInputShape.UNI, awaitPayload);
        String transitionKey = "await-item-continuation:" + unit.unitId() + ":" + record.itemIndex();
        TransitionWorkerCommand workerCommand = new TransitionWorkerCommand(
            parent.get().tenantId(),
            parent.get().executionId(),
            nextStepIndex,
            aggregateStepIndex,
            parent.get().attempt(),
            ExecutionResultShape.MATERIALIZED_MULTI,
            parent.get().version(),
            transitionKey,
            continuationInput);
        TransitionCommandEnvelope envelope = TransitionCommandEnvelope.from(
            workerCommand,
            parent.get().pipelineId(),
            parent.get().contractVersion(),
            parent.get().releaseVersion(),
            transitionKey,
            payloadCodec().encode(continuationInput));
        return transitionWorkerExecutor().execute(selectedWorker, envelope)
            .onItem().transformToUni(result -> {
              if (result.outcome() != TransitionWorkerOutcome.COMPLETED) {
                return Uni.createFrom().failure(new IllegalStateException(
                    "Await item continuation transition did not complete: " + result.outcome()));
              }
              return queueAsyncCoordinator.recordAwaitItemContinuation(
                record,
                unit,
                aggregateStepIndex,
                continuationInput,
                result.decodeOutputItems(payloadCodec()),
                nowEpochMs);
            });
      }

      @Override
      public Uni<Void> releaseAwaitParentIfReady(
          ExecutionRecord<Object, Object> parent,
          AwaitUnitRecord unit,
          int nextStepIndex,
          long nowEpochMs) {
        return releaseItemizedAwaitParentIfReady(parent, unit, nextStepIndex, nowEpochMs);
      }
    };
  }

  /**
   * Queries pending durable await interactions.
   */
  public Uni<List<AwaitInteractionRecord>> queryPendingAwaitInteractions(
      String tenantId,
      String assignee,
      String group,
      String stepId,
      int limit) {
    return controlPlane.queryPendingAwaitInteractions(
        tenantId,
        normalizeBlankFilter(assignee),
        normalizeBlankFilter(group),
        normalizeBlankFilter(stepId),
        limit);
  }

  private static String normalizeBlankFilter(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
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
    PipelineTransitionWorker selectedWorker = transitionWorkerSelector.select(this);
    return controlPlane.processExecutionWorkItem(workItem, selectedWorker, awaitItemContinuationHandler(selectedWorker));
  }

  /**
   * Executes one local transition and converts runtime control flow into an explicit worker result.
   *
   * @param command transition command
   * @return worker result
   */
  @Override
  public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
    return executeTransition(command, false);
  }

  /**
   * Executes one transition and returns a wire-portable encoded result envelope.
   *
   * @param command transition command
   * @return encoded worker result
   */
  public Uni<TransitionResultEnvelope> executePortableTransition(TransitionCommandEnvelope command) {
    return executeTransition(command, true);
  }

  private Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command, boolean encodeOutputs) {
    var identityMismatch = validateCommandIdentity(command, !encodeOutputs);
    if (identityMismatch.isPresent()) {
      return Uni.createFrom().item(TransitionResultEnvelope.failed(new IllegalArgumentException(identityMismatch.get())));
    }
    TransitionWorkerCommand decodedCommand;
    try {
      decodedCommand = command.toCommand(payloadCodec());
    } catch (Throwable failure) {
      return Uni.createFrom().item(TransitionResultEnvelope.failed(failure));
    }
    return executePipelineStreamingFromCommand(decodedCommand)
        .collect().asList()
        .onItem().transform(items -> encodeOutputs
            ? TransitionResultEnvelope.completed(payloadCodec(), items)
            : TransitionResultEnvelope.completedInProcess(items))
        .onFailure(AwaitThrowableSupport::containsAwaitSuspension).recoverWithUni(failure -> {
          AwaitSuspendedException suspended = AwaitThrowableSupport.extractAwaitSuspension(failure);
          return awaitCoordinator.suspensionSnapshot(suspended)
              .onItem().transform(TransitionResultEnvelope::waiting);
        })
        .onFailure().recoverWithItem(TransitionResultEnvelope::failed);
  }

  private java.util.Optional<String> validateCommandIdentity(
      TransitionCommandEnvelope command,
      boolean allowLocalFallbackIdentity) {
    if (allowLocalFallbackIdentity
        && PipelineContractDescriptor.DEFAULT_PIPELINE_ID.equals(command.pipelineId())
        && PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION.equals(command.contractVersion())
        && PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION.equals(command.releaseVersion())) {
      return java.util.Optional.empty();
    }
    return releaseIdentityResolver().validateCommandIdentity(command, orchestratorConfig);
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
      RuntimeException healthFailure = healthCheckFailure();
      if (healthFailure != null) {
        return Multi.createFrom().failure(healthFailure);
      }
      RuntimeException inputFailure = validateInputShape(input);
      if (inputFailure != null) {
        return Multi.createFrom().failure(inputFailure);
      }

      PipelineRunner.ExecutionResult executionResult = pipelineRunner.runWithContext(input, steps);
      Object result = executionResult.result();
      if (result == null) {
        return Multi.createFrom().failure(new IllegalStateException("PipelineRunner returned null"));
      } else if (result instanceof Multi<?> multi) {
        return executionHooks.attachMultiHooks(multi, watch, executionResult.telemetryContext());
      } else if (result instanceof Uni<?> uni) {
        return executionHooks.attachMultiHooks(uni.toMulti(), watch, executionResult.telemetryContext());
      } else {
        return Multi.createFrom().failure(new IllegalStateException(
            MessageFormat.format("PipelineRunner returned unexpected type: {0}", result.getClass().getName())));
      }
    });
  }

  private Multi<?> executePipelineStreamingFromCommand(TransitionWorkerCommand command) {
    return Multi.createFrom().deferred(() -> {
      Uni<Object> sourcePayload = Uni.createFrom().item(command.inputPayload());
      return sourcePayload.onItem().transformToMulti(payload -> {
        Object reactiveInput = executionInputPolicy.toReplayInput(payload);
        AwaitExecutionContext previous = AwaitExecutionContextHolder.get();
        AwaitExecutionContextHolder.set(new AwaitExecutionContext(
            command.tenantId(),
            command.executionId(),
            command.currentStepIndex()));
        try {
          RuntimeException healthFailure = healthCheckFailure();
          if (healthFailure != null) {
            restoreAwaitContext(previous);
            return Multi.createFrom().failure(healthFailure);
          }
          RuntimeException inputFailure = validateInputShape(reactiveInput);
          if (inputFailure != null) {
            restoreAwaitContext(previous);
            return Multi.createFrom().failure(inputFailure);
          }
          List<Object> steps = loadStepsForExecution();
          int requestedStopBeforeStepIndex = command.stopBeforeStepIndex();
          if (requestedStopBeforeStepIndex > steps.size()) {
            restoreAwaitContext(previous);
            return Multi.createFrom().failure(new IllegalArgumentException(
                "stopBeforeStepIndex " + requestedStopBeforeStepIndex
                    + " exceeds pipeline step count " + steps.size()));
          }
          int stopBeforeStepIndex = requestedStopBeforeStepIndex < 0
              ? steps.size()
              : requestedStopBeforeStepIndex;
          if (stopBeforeStepIndex == command.currentStepIndex()) {
            return reactiveInput instanceof Multi<?> multi
                ? multi
                : ((Uni<?>) reactiveInput).toMulti();
          }
          Object result = executePipelineStreamingInternalFromStep(
              reactiveInput,
              steps,
              command.currentStepIndex(),
              stopBeforeStepIndex);
          Multi<?> stream;
          if (result instanceof Multi<?> multi) {
            stream = multi;
          } else if (result instanceof Uni<?> uni) {
            stream = uni.toMulti();
          } else {
            restoreAwaitContext(previous);
            return Multi.createFrom().failure(new IllegalStateException("Pipeline runner returned unsupported result"));
          }
          return stream.onTermination().invoke((failure, cancelled) -> restoreAwaitContext(previous));
        } catch (Throwable failure) {
          restoreAwaitContext(previous);
          return Multi.createFrom().failure(failure);
        }
      });
    });
  }

  private TransitionPayloadCodec payloadCodec() {
    if (transitionPayloadCodec != null) {
      return transitionPayloadCodec;
    }
    TransitionPayloadCodec fallback = fallbackPayloadCodec;
    if (fallback == null) {
      synchronized (this) {
        fallback = fallbackPayloadCodec;
        if (fallback == null) {
          fallback = new JsonTransitionPayloadCodec();
          fallbackPayloadCodec = fallback;
        }
      }
    }
    return fallback;
  }

  private TransitionWorkerExecutor transitionWorkerExecutor() {
    if (transitionWorkerExecutor != null) {
      return transitionWorkerExecutor;
    }
    throw new IllegalStateException("TransitionWorkerExecutor is not available for await item continuation dispatch");
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

  private Object executePipelineStreamingInternalFromStep(Object input, int startStepIndex) {
    List<Object> steps = loadStepsForExecution();
    return executePipelineStreamingInternalFromStep(input, steps, startStepIndex, steps.size());
  }

  private Object executePipelineStreamingInternalFromStep(
      Object input,
      List<Object> steps,
      int startStepIndex,
      int stopBeforeStepIndex) {
    StopWatch watch = new StopWatch();
    if (steps == null) {
      return Multi.createFrom().failure(new IllegalStateException("Pipeline steps could not be loaded."));
    }
    PipelineRunner.ExecutionResult executionResult =
        pipelineRunner.runFromStepUntilWithContext(input, steps, startStepIndex, stopBeforeStepIndex);
    Object result = executionResult.result();
    if (result instanceof Multi<?> multi) {
      return executionHooks.attachMultiHooks(multi, watch, executionResult.telemetryContext());
    }
    if (result instanceof Uni<?> uni) {
      return executionHooks.attachMultiHooks(uni.toMulti(), watch, executionResult.telemetryContext());
    }
    String resultType = result == null ? "null" : result.getClass().getName();
    Multi<?> failed = Multi.createFrom().failure(new IllegalStateException(
        MessageFormat.format(
            "PipelineRunner returned unexpected type from step index {0}: {1}",
            startStepIndex,
            resultType)));
    return executionHooks.attachMultiHooks(failed, watch, executionResult.telemetryContext());
  }

  private static int firstAggregateStepIndex(List<Object> steps, int startStepIndex) {
    if (steps == null) {
      return startStepIndex;
    }
    for (int index = Math.max(0, startStepIndex); index < steps.size(); index++) {
      Object step = steps.get(index);
      if (step instanceof ManyToOne<?, ?> || step instanceof StepManyToMany<?, ?>) {
        return index;
      }
    }
    return steps.size();
  }

  private Object coerceAwaitItemPayload(AwaitInteractionRecord record) {
    try {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      if (loader == null) {
        loader = AwaitPayloadSupport.class.getClassLoader();
      }
      Class<?> outputType = AwaitPayloadSupport.resolvePayloadClass(
          record.outputType(),
          loader);
      return AwaitPayloadSupport.coercePayload(record.responsePayload(), outputType);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "Failed resolving await output type " + record.outputType()
              + " for interaction " + record.interactionId(),
          e);
    }
  }

  private Uni<Void> releaseItemizedAwaitParentIfReady(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int nextStepIndex,
      long nowEpochMs) {
    List<Object> steps = loadStepsForExecution();
    List<Object> orderedSteps = stepOrderer.orderSteps(steps);
    int aggregateStepIndex = firstAggregateStepIndex(orderedSteps, nextStepIndex);
    return queueAsyncCoordinator.releaseItemizedAwaitParentIfReady(
        parent,
        unit,
        aggregateStepIndex,
        nowEpochMs);
  }

  private void restoreAwaitContext(AwaitExecutionContext previous) {
    if (previous == null) {
      AwaitExecutionContextHolder.clear();
    } else {
      AwaitExecutionContextHolder.set(previous);
    }
  }

  private Uni<?> executePipelineUnaryInternal(Object input) {
    return Uni.createFrom().deferred(() -> {
      StopWatch watch = new StopWatch();
      List<Object> steps = loadStepsForExecution();
      RuntimeException healthFailure = healthCheckFailure();
      if (healthFailure != null) {
        return Uni.createFrom().failure(healthFailure);
      }
      RuntimeException inputFailure = validateInputShape(input);
      if (inputFailure != null) {
        return Uni.createFrom().failure(inputFailure);
      }

      PipelineRunner.ExecutionResult executionResult = pipelineRunner.runWithContext(input, steps);
      Object result = executionResult.result();
      return switch (result) {
        case null -> Uni.createFrom().failure(new IllegalStateException("PipelineRunner returned null"));
        case Uni<?> uni -> executionHooks.attachUniHooks(uni, watch, executionResult.telemetryContext());
        case Multi<?> ignored -> Uni.createFrom().failure(new IllegalStateException(
            "PipelineRunner returned stream output where unary output was expected"));
        default -> Uni.createFrom().failure(new IllegalStateException(
            MessageFormat.format("PipelineRunner returned unexpected type: {0}", result.getClass().getName())));
      };
    });
  }

  private List<Object> loadStepsForExecution() {
    try {
      List<Object> steps = loadPipelineSteps();
      initialiseConfigurableSteps(steps);
      return steps;
    } catch (PipelineConfigurationException e) {
      LOG.errorf(e, "Failed to load pipeline configuration: %s", e.getMessage());
      throw e;
    }
  }

  private void initialiseConfigurableSteps(List<Object> steps) {
    if (steps == null) {
      return;
    }
    for (Object step : steps) {
      if (step instanceof Configurable configurable) {
        configurable.initialiseWithConfig(configFactory.buildConfig(step.getClass(), pipelineConfig));
      }
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
