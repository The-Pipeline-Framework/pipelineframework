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

class QueueAsyncSweepFlow {

  private static final Logger LOG = Logger.getLogger(QueueAsyncSweepFlow.class);

  private final PipelineOrchestratorConfig orchestratorConfig;
  private final ExecutionStateStore executionStateStore;
  private final WorkDispatcher workDispatcher;
  private final AwaitTimeoutFlow awaitTimeoutFlow;

  QueueAsyncSweepFlow(
      PipelineOrchestratorConfig orchestratorConfig,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      AwaitTimeoutFlow awaitTimeoutFlow) {
    this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");
    this.executionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    this.workDispatcher = Objects.requireNonNull(workDispatcher, "workDispatcher must not be null");
    this.awaitTimeoutFlow = Objects.requireNonNull(awaitTimeoutFlow, "awaitTimeoutFlow must not be null");
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
        .chain(() -> executionStateStore.findDueExecutions(nowEpochMs, limit))
        .onItem().transform(DueExecutionDispatchPlan::from)
        .onItem().transformToUni(this::dispatchDueExecutions);
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
