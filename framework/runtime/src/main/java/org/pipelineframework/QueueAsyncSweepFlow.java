package org.pipelineframework;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;
import org.pipelineframework.stream.StreamRegionContinuationRegistry;

class QueueAsyncSweepFlow {

  private static final Logger LOG = Logger.getLogger(QueueAsyncSweepFlow.class);

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final AwaitTimeoutFlow awaitTimeoutFlow;
  private final AwaitCoordinator awaitCoordinator;
  private final StreamRegionContinuationRegistry streamRegionContinuations;

  QueueAsyncSweepFlow(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitTimeoutFlow awaitTimeoutFlow) {
    this(orchestratorConfig, executionStateStore, workDispatcher, awaitTimeoutFlow, null, null);
  }

  QueueAsyncSweepFlow(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitTimeoutFlow awaitTimeoutFlow,
      AwaitCoordinator awaitCoordinator) {
    this(orchestratorConfig, executionStateStore, workDispatcher, awaitTimeoutFlow, awaitCoordinator, null);
  }

  QueueAsyncSweepFlow(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitTimeoutFlow awaitTimeoutFlow,
      AwaitCoordinator awaitCoordinator,
      StreamRegionContinuationRegistry streamRegionContinuations) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.awaitTimeoutFlow = Objects.requireNonNull(awaitTimeoutFlow, "awaitTimeoutFlow must not be null");
    this.awaitCoordinator = awaitCoordinator;
    this.streamRegionContinuations = streamRegionContinuations;
  }

  void sweepDueExecutions() {
    sweepOnce(System.currentTimeMillis())
        .subscribe()
        .with(
            ignored -> {
            },
            failure -> LOG.errorf(failure, "Failed sweeping due async executions"));
  }

    Uni<Void> sweepOnce(long nowEpochMs) {
        int limit = orchestratorConfig.sweepLimit();
        return awaitTimeoutFlow.sweepTimedOut(nowEpochMs, limit)
        .chain(() -> dispatchDueStreamInteractionDispatches(nowEpochMs, limit))
        .chain(() -> dispatchDueContinuations(nowEpochMs, limit))
        .chain(() -> dispatchDueStreamRegions(nowEpochMs, limit))
        .chain(() -> executionStateStore.findDueExecutions(nowEpochMs, limit))
        .onItem().transform(DueExecutionDispatchPlan::from)
        .onItem().transformToUni(this::dispatchDueExecutions);
  }

  private Uni<Void> dispatchDueStreamInteractionDispatches(long nowEpochMs, int limit) {
    if (awaitCoordinator == null || streamRegionContinuations == null) {
      return Uni.createFrom().voidItem();
    }
    return awaitCoordinator.findDueStreamInteractionDispatches(nowEpochMs, limit)
        .onItem().transformToMulti(records -> Multi.createFrom().iterable(records))
        .onItem().transformToUniAndConcatenate(this::dispatchDueStreamInteraction)
        .collect().asList()
        .replaceWithVoid();
  }

  private Uni<Void> dispatchDueStreamInteraction(AwaitInteractionRecord interaction) {
    return executionStateStore.getStreamRegion(
            interaction.tenantId(), interaction.executionId(), interaction.streamRegionId())
        .onItem().transformToUni(region -> region
            .map(record -> streamRegionContinuations.find(record.source())
                .map(continuation -> awaitCoordinator.dispatch(continuation.awaitBinding().descriptor(), interaction))
                .orElseGet(() -> Uni.createFrom().failure(new IllegalStateException(
                    "No generated stream continuation matches persisted source " + record.source())))
            .replaceWithVoid())
            .orElseGet(() -> Uni.createFrom().failure(new IllegalStateException(
                "Stream region is missing for persisted await interaction " + interaction.interactionId()))));
  }

  private Uni<Void> dispatchDueContinuations(long nowEpochMs, int limit) {
    if (awaitCoordinator == null) {
      return Uni.createFrom().voidItem();
    }
    return awaitCoordinator.findDueItemContinuations(nowEpochMs, limit)
        .onItem().transformToMulti(records -> Multi.createFrom().iterable(records))
        .onItem().transformToUniAndConcatenate(this::enqueueDueContinuation)
        .collect().asList()
        .replaceWithVoid();
  }

    private Uni<Void> enqueueDueContinuation(AwaitInteractionRecord interaction) {
    if (interaction.streamRegionId().isBlank()) {
      return Uni.createFrom().voidItem();
    }
    return workDispatcher.enqueueNow(ExecutionWorkItem.awaitContinuation(
        interaction.tenantId(), interaction.executionId(), interaction.interactionId()));
  }

  private Uni<Void> dispatchDueStreamRegions(long nowEpochMs, int limit) {
    return executionStateStore.findDueStreamRegions(nowEpochMs, limit)
        .onItem().transformToMulti(regions -> Multi.createFrom().iterable(regions))
        .onItem().transformToUniAndConcatenate(this::enqueueDueStreamRegion)
        .collect().asList()
        .replaceWithVoid();
  }

  private Uni<Void> enqueueDueStreamRegion(StreamRegionRecord region) {
    return workDispatcher.enqueueNow(ExecutionWorkItem.streamRegion(
        region.tenantId(), region.executionId(), region.regionId()));
  }

  private Uni<Void> dispatchDueExecutions(DueExecutionDispatchPlan plan) {
    if (plan.empty()) {
      return Uni.createFrom().voidItem();
    }
    return Multi.createFrom().iterable(plan.workItems())
        .onItem().transformToUniAndConcatenate(this::enqueueDueExecution)
        .collect().asList()
        .onItem().transformToUni(this::failIfAnyDispatchFailed);
  }

  private Uni<Optional<Throwable>> enqueueDueExecution(ExecutionWorkItem item) {
    return workDispatcher.enqueueNow(item)
        .replaceWith(Optional.<Throwable>empty())
        .onFailure().recoverWithItem(failure -> Optional.of(new IllegalStateException(
            "Failed to re-dispatch due execution " + item.executionId(),
            failure)));
  }

  private Uni<Void> failIfAnyDispatchFailed(List<Optional<Throwable>> results) {
    List<Throwable> failures = results.stream()
        .flatMap(Optional::stream)
        .toList();
    if (failures.isEmpty()) {
      return Uni.createFrom().voidItem();
    }
    if (failures.size() == 1) {
      return Uni.createFrom().failure(failures.get(0));
    }
    IllegalStateException combined = new IllegalStateException(
        "Failed to re-dispatch " + failures.size() + " due executions",
        failures.get(0));
    failures.stream().skip(1).forEach(combined::addSuppressed);
    return Uni.createFrom().failure(combined);
  }
}
