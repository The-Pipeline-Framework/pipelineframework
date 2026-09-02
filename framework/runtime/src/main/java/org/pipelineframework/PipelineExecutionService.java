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
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitContinuationMode;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.awaitable.AwaitThrowableSupport;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.TerminalOutputOwnership;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.command.CommandReexecutionScope;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
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
  PipelineStepConfig pipelineStepConfig;

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
        Object awaitPayload = awaitCoordinator.resumePayload(record);
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
    return executeTransition(command, TransitionExecutionPolicy.IN_PROCESS);
  }

  /**
   * Executes one transition and returns a wire-portable encoded result envelope.
   *
   * @param command transition command
   * @return encoded worker result
   */
  public Uni<TransitionResultEnvelope> executePortableTransition(TransitionCommandEnvelope command) {
    return executeTransition(command, portableExecutionPolicy(command));
  }

  /**
   * Keeps a portable itemized await live only for the narrow generated shape that can complete its
   * scalar suffix in this transition worker. Every missing, malformed, or ineligible contract
   * remains on the durable handoff path.
   */
  private TransitionExecutionPolicy portableExecutionPolicy(TransitionCommandEnvelope command) {
    try {
      PipelineContractDescriptor contract = releaseIdentityResolver().contract();
      return isPortableLiveItemizedAwait(command, contract)
          ? TransitionExecutionPolicy.PORTABLE_LIVE_ITEMIZED
          : TransitionExecutionPolicy.PORTABLE;
    } catch (RuntimeException failure) {
      LOG.debugf(failure, "Falling back to durable portable await execution because live eligibility could not be resolved");
      return TransitionExecutionPolicy.PORTABLE;
    }
  }

  private static boolean isPortableLiveItemizedAwait(
      TransitionCommandEnvelope command,
      PipelineContractDescriptor contract) {
    if (command == null || contract == null || contract.steps() == null) {
      return false;
    }
    List<PipelineBundleStepDescriptor> steps = contract.steps();
    int producerIndex = command.currentStepIndex();
    int stopBeforeStepIndex = command.stopBeforeStepIndex() < 0 ? steps.size() : command.stopBeforeStepIndex();
    if (producerIndex < 0
        || stopBeforeStepIndex != steps.size()
        || producerIndex + 2 >= stopBeforeStepIndex) {
      return false;
    }

    PipelineBundleStepDescriptor producer = steps.get(producerIndex);
    PipelineBundleStepDescriptor await = steps.get(producerIndex + 1);
    if (producer.index() != producerIndex
        || await.index() != producerIndex + 1
        || !hasCardinality(producer, CardinalitySemantics.ONE_TO_MANY)
        || !"await".equalsIgnoreCase(await.kind())
        || !hasCardinality(await, CardinalitySemantics.ONE_TO_ONE)
        || !sameCanonicalType(producer.outputTypeId(), await.inputTypeId())) {
      return false;
    }

    for (int index = producerIndex + 2; index < stopBeforeStepIndex; index++) {
      PipelineBundleStepDescriptor suffix = steps.get(index);
      if (suffix.index() != index
          || !hasCardinality(suffix, CardinalitySemantics.ONE_TO_ONE)
          || "await".equalsIgnoreCase(suffix.kind())) {
        return false;
      }
    }
    return true;
  }

  private static boolean sameCanonicalType(String left, String right) {
    return left != null && !left.isBlank() && left.equals(right);
  }

  private static boolean hasCardinality(
      PipelineBundleStepDescriptor descriptor,
      CardinalitySemantics expected) {
    try {
      return expected == CardinalitySemantics.fromString(descriptor.cardinality());
    } catch (IllegalArgumentException ignored) {
      return false;
    }
  }

  private Uni<TransitionResultEnvelope> executeTransition(
      TransitionCommandEnvelope command,
      TransitionExecutionPolicy policy) {
    var identityMismatch = validateCommandIdentity(command, policy.allowLocalFallbackIdentity());
    if (identityMismatch.isPresent()) {
      return Uni.createFrom().item(TransitionResultEnvelope.failed(new IllegalArgumentException(identityMismatch.get())));
    }
    TransitionWorkerCommand decodedCommand;
    try {
      decodedCommand = command.toCommand(payloadCodec());
    } catch (Throwable failure) {
      return Uni.createFrom().item(TransitionResultEnvelope.failed(failure));
    }
    AtomicBoolean terminalOutputPublished = new AtomicBoolean(false);
    AtomicBoolean terminalInputPassthrough = new AtomicBoolean(false);
    return executePipelineStreamingFromCommand(
            decodedCommand,
            command,
            terminalOutputPublished,
            terminalInputPassthrough,
            policy.continuationMode(),
            policy.terminalOutputOwnership())
        .collect().asList()
        .onItem().transform(items -> {
          if (terminalInputPassthrough.get()) {
            return TransitionResultEnvelope.completedTerminalInputPassthrough();
          }
          boolean published = terminalOutputPublished.get();
          return policy.encodeOutputs()
              ? TransitionResultEnvelope.completed(payloadCodec(), published ? List.of() : items, published)
              : TransitionResultEnvelope.completedInProcess(items, published);
        })
        .onFailure(AwaitThrowableSupport::containsAwaitSuspension).recoverWithUni(failure -> {
          AwaitSuspendedException suspended = AwaitThrowableSupport.extractAwaitSuspension(failure);
          return awaitCoordinator.suspensionSnapshot(suspended)
              .onItem().transform(TransitionResultEnvelope::waiting);
        })
        .onFailure().recoverWithItem(failure -> TransitionResultEnvelope.failed(
            PipelineStepExecutionFailure.source(failure),
            PipelineStepExecutionFailure.stepIndex(failure)));
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
      RuntimeException inputFailure = validateInputShape(input);
      if (inputFailure != null) {
        return Multi.createFrom().failure(inputFailure);
      }
      return awaitStartupHealthReactive().onItem().transformToMulti(ignored -> {
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
    });
  }

  private Multi<?> executePipelineStreamingFromCommand(
      TransitionWorkerCommand command,
      TransitionCommandEnvelope envelope,
      AtomicBoolean terminalOutputPublished,
      AtomicBoolean terminalInputPassthrough,
      AwaitContinuationMode continuationMode,
      TerminalOutputOwnership terminalOutputOwnership) {
    return Multi.createFrom().deferred(() -> {
      Uni<Object> sourcePayload = Uni.createFrom().item(command.inputPayload());
      return sourcePayload.onItem().transformToMulti(payload -> {
        ExecutionInputPolicy.RehydratedExecutionInput rehydratedInput =
            executionInputPolicy.rehydrateExecutionInput(payload);
        Object reactiveInput = rehydratedInput.reactiveInput();
        PipelineContext previousPipeline = PipelineContextHolder.get();
        AwaitExecutionContext previous = AwaitExecutionContextHolder.get();
        java.util.Optional<PipelineExecutionContext> previousExecution = PipelineExecutionContextHolder.get();
        CommandReexecutionScope.Snapshot previousCommandRetry = CommandReexecutionScope.capture();
        PipelineExecutionContext executionContext = new PipelineExecutionContext(
            command.tenantId(),
            command.executionId(),
            envelope.pipelineId(),
            envelope.contractVersion(),
            envelope.releaseVersion(),
            command.currentStepIndex(),
            java.util.Optional.empty(),
            java.util.Optional.of(envelope.traceId()).filter(traceId -> !traceId.isBlank()));
        final CommandReexecutionScope.AdmissionHandle commandRetryAdmission;
        if (command.redriveIntent() == org.pipelineframework.orchestrator.ExecutionRedriveIntent.RETRY_FAILED_COMMAND) {
          commandRetryAdmission = CommandReexecutionScope.installRetry(
              command.redriveCommandId().orElseThrow(), command.transitionKey());
        } else if (command.redriveIntent()
            == org.pipelineframework.orchestrator.ExecutionRedriveIntent.REISSUE_COMMAND) {
          commandRetryAdmission = CommandReexecutionScope.installReissue(
              command.redriveCommandId().orElseThrow(),
              command.transitionKey(),
              command.redriveReason().orElseThrow(() -> new IllegalStateException(
                  "REISSUE_COMMAND worker command is missing its audit reason")));
        } else {
          CommandReexecutionScope.clear();
          commandRetryAdmission = null;
        }
        AwaitExecutionContextHolder.set(new AwaitExecutionContext(
            command.tenantId(),
            command.executionId(),
            command.currentStepIndex(),
            continuationMode,
            terminalOutputOwnership,
            java.util.Map.of()));
        PipelineExecutionContextHolder.set(executionContext);
        rehydratedInput.pipelineContext().ifPresentOrElse(
            PipelineContextHolder::set,
            PipelineContextHolder::clear);
        try {
          RuntimeException healthFailure = healthCheckFailure();
          if (healthFailure != null) {
            restoreExecutionContexts(previousPipeline, previous, previousExecution, previousCommandRetry);
            return Multi.createFrom().failure(healthFailure);
          }
          RuntimeException inputFailure = validateInputShape(reactiveInput);
          if (inputFailure != null) {
            restoreExecutionContexts(previousPipeline, previous, previousExecution, previousCommandRetry);
            return Multi.createFrom().failure(inputFailure);
          }
          List<Object> steps = loadStepsForExecution();
          int requestedStopBeforeStepIndex = command.stopBeforeStepIndex();
          if (requestedStopBeforeStepIndex > steps.size()) {
            restoreExecutionContexts(previousPipeline, previous, previousExecution, previousCommandRetry);
            return Multi.createFrom().failure(new IllegalArgumentException(
                "stopBeforeStepIndex " + requestedStopBeforeStepIndex
                    + " exceeds pipeline step count " + steps.size()));
          }
          int stopBeforeStepIndex = requestedStopBeforeStepIndex < 0
              ? steps.size()
              : requestedStopBeforeStepIndex;
          if (stopBeforeStepIndex == command.currentStepIndex()) {
            if (terminalOutputOwnership == TerminalOutputOwnership.COORDINATOR
                && command.currentStepIndex() == steps.size()) {
              terminalInputPassthrough.set(true);
              return requireCommandRetryConsumed(Multi.createFrom().empty(), commandRetryAdmission)
                  .onTermination().invoke((failure, cancelled) ->
                      restoreExecutionContexts(previousPipeline, previous, previousExecution, previousCommandRetry));
            }
            Multi<?> unchanged = reactiveInput instanceof Multi<?> multi
                ? multi
                : ((Uni<?>) reactiveInput).toMulti();
            return requireCommandRetryConsumed(unchanged, commandRetryAdmission)
                .onTermination().invoke((failure, cancelled) ->
                    restoreExecutionContexts(previousPipeline, previous, previousExecution, previousCommandRetry));
          }
          PipelineRunner.ExecutionResult executionResult = executePipelineStreamingInternalFromStep(
              reactiveInput,
              steps,
              command.currentStepIndex(),
              stopBeforeStepIndex);
          terminalOutputPublished.set(executionResult.terminalOutputPublished());
          Object result = executionResult.result();
          Multi<?> stream;
          if (result instanceof Multi<?> multi) {
            stream = multi;
          } else if (result instanceof Uni<?> uni) {
            stream = uni.toMulti();
          } else {
            restoreExecutionContexts(previousPipeline, previous, previousExecution, previousCommandRetry);
            return Multi.createFrom().failure(new IllegalStateException("Pipeline runner returned unsupported result"));
          }
          return requireCommandRetryConsumed(stream, commandRetryAdmission)
              .onTermination().invoke((failure, cancelled) ->
                  restoreExecutionContexts(previousPipeline, previous, previousExecution, previousCommandRetry));
        } catch (Throwable failure) {
          restoreExecutionContexts(previousPipeline, previous, previousExecution, previousCommandRetry);
          return Multi.createFrom().failure(failure);
        }
      });
    });
  }

  /**
   * Separates portable transition encoding from live-await eligibility and terminal output ownership.
   */
  private enum TransitionExecutionPolicy {
    IN_PROCESS(
        false,
        true,
        AwaitContinuationMode.LIVE_IF_SUPPORTED,
        TerminalOutputOwnership.TRANSITION_WORKER),
    PORTABLE(
        true,
        false,
        AwaitContinuationMode.DURABLE_HANDOFF,
        TerminalOutputOwnership.COORDINATOR),
    PORTABLE_LIVE_ITEMIZED(
        true,
        false,
        AwaitContinuationMode.LIVE_IF_SUPPORTED,
        TerminalOutputOwnership.TRANSITION_WORKER);

    private final boolean encodeOutputs;
    private final boolean allowLocalFallbackIdentity;
    private final AwaitContinuationMode continuationMode;
    private final TerminalOutputOwnership terminalOutputOwnership;

    TransitionExecutionPolicy(
        boolean encodeOutputs,
        boolean allowLocalFallbackIdentity,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership) {
      this.encodeOutputs = encodeOutputs;
      this.allowLocalFallbackIdentity = allowLocalFallbackIdentity;
      this.continuationMode = continuationMode;
      this.terminalOutputOwnership = terminalOutputOwnership;
    }

    boolean encodeOutputs() {
      return encodeOutputs;
    }

    boolean allowLocalFallbackIdentity() {
      return allowLocalFallbackIdentity;
    }

    AwaitContinuationMode continuationMode() {
      return continuationMode;
    }

    TerminalOutputOwnership terminalOutputOwnership() {
      return terminalOutputOwnership;
    }
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

  private PipelineRunner.ExecutionResult executePipelineStreamingInternalFromStep(Object input, int startStepIndex) {
    List<Object> steps = loadStepsForExecution();
    return executePipelineStreamingInternalFromStep(input, steps, startStepIndex, steps.size());
  }

  private PipelineRunner.ExecutionResult executePipelineStreamingInternalFromStep(
      Object input,
      List<Object> steps,
      int startStepIndex,
      int stopBeforeStepIndex) {
    StopWatch watch = new StopWatch();
    if (steps == null) {
      return new PipelineRunner.ExecutionResult(
          Multi.createFrom().failure(new IllegalStateException("Pipeline steps could not be loaded.")),
          null);
    }
    PipelineRunner.ExecutionResult executionResult =
        pipelineRunner.runFromStepUntilWithContext(input, steps, startStepIndex, stopBeforeStepIndex);
    Object result = executionResult.result();
    if (result instanceof Multi<?> multi) {
      return new PipelineRunner.ExecutionResult(
          executionHooks.attachMultiHooks(multi, watch, executionResult.telemetryContext()),
          executionResult.telemetryContext(),
          executionResult.terminalOutputPublished());
    }
    if (result instanceof Uni<?> uni) {
      return new PipelineRunner.ExecutionResult(
          executionHooks.attachMultiHooks(uni.toMulti(), watch, executionResult.telemetryContext()),
          executionResult.telemetryContext(),
          executionResult.terminalOutputPublished());
    }
    String resultType = result == null ? "null" : result.getClass().getName();
    Multi<?> failed = Multi.createFrom().failure(new IllegalStateException(
        MessageFormat.format(
            "PipelineRunner returned unexpected type from step index {0}: {1}",
            startStepIndex,
            resultType)));
    return new PipelineRunner.ExecutionResult(
        executionHooks.attachMultiHooks(failed, watch, executionResult.telemetryContext()),
        executionResult.telemetryContext(),
        executionResult.terminalOutputPublished());
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

  private void restoreExecutionContexts(
      PipelineContext previousPipeline,
      AwaitExecutionContext previousAwait,
      java.util.Optional<PipelineExecutionContext> previousExecution,
      CommandReexecutionScope.Snapshot previousCommandRetry) {
    if (previousPipeline == null) {
      PipelineContextHolder.clear();
    } else {
      PipelineContextHolder.set(previousPipeline);
    }
    restoreAwaitContext(previousAwait);
    previousExecution.ifPresentOrElse(
        PipelineExecutionContextHolder::set,
        PipelineExecutionContextHolder::clear);
    CommandReexecutionScope.restore(previousCommandRetry);
  }

  private Multi<?> requireCommandRetryConsumed(
      Multi<?> stream,
      CommandReexecutionScope.AdmissionHandle admission) {
    return admission == null ? stream : stream.onCompletion().invoke(admission::requireConsumed);
  }

  private Uni<?> executePipelineUnaryInternal(Object input) {
    return Uni.createFrom().deferred(() -> {
      StopWatch watch = new StopWatch();
      List<Object> steps = loadStepsForExecution();
      RuntimeException inputFailure = validateInputShape(input);
      if (inputFailure != null) {
        return Uni.createFrom().failure(inputFailure);
      }
      return awaitStartupHealthReactive().onItem().transformToUni(ignored -> {
        PipelineRunner.ExecutionResult executionResult = pipelineRunner.runWithContext(input, steps);
        Object result = executionResult.result();
        return switch (result) {
          case null -> Uni.createFrom().failure(new IllegalStateException("PipelineRunner returned null"));
          case Uni<?> uni -> executionHooks.attachUniHooks(uni, watch, executionResult.telemetryContext());
          case Multi<?> ignoredResult -> Uni.createFrom().failure(new IllegalStateException(
              "PipelineRunner returned stream output where unary output was expected"));
          default -> Uni.createFrom().failure(new IllegalStateException(
              MessageFormat.format("PipelineRunner returned unexpected type: {0}", result.getClass().getName())));
        };
      });
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

  private Uni<Void> awaitStartupHealthReactive() {
    StartupHealthState state = startupHealthState.get();
    if (state == StartupHealthState.HEALTHY) {
      return Uni.createFrom().voidItem();
    }
    if (state != StartupHealthState.PENDING) {
      return Uni.createFrom().failure(new RuntimeException(
          "One or more dependent services are not healthy. Pipeline execution aborted (" + state + ")."));
    }
    CompletableFuture<Boolean> future = startupHealthFuture;
    if (future == null) {
      return Uni.createFrom().failure(new RuntimeException("Startup health checks are unavailable."));
    }
    return Uni.createFrom().completionStage(future)
        .ifNoItem().after(pipelineStepConfig.health().startupTimeout())
        .failWith(() -> new RuntimeException("Startup health checks are still running."))
        .onItem().transformToUni(healthy -> {
          if (Boolean.TRUE.equals(healthy)) {
            startupHealthState.compareAndSet(StartupHealthState.PENDING, StartupHealthState.HEALTHY);
            return Uni.createFrom().voidItem();
          }
          StartupHealthState resolved = startupHealthState.get();
          if (resolved == StartupHealthState.PENDING) {
            resolved = StartupHealthState.UNHEALTHY;
            startupHealthState.compareAndSet(StartupHealthState.PENDING, resolved);
          }
          return Uni.createFrom().failure(new RuntimeException(
              "One or more dependent services are not healthy. Pipeline execution aborted (" + resolved + ")."));
        });
  }

  private RuntimeException healthCheckFailure() {
    StartupHealthState state = startupHealthState.get();
    if (state == StartupHealthState.PENDING) {
      try {
        awaitStartupHealth(pipelineStepConfig.health().startupTimeout());
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
