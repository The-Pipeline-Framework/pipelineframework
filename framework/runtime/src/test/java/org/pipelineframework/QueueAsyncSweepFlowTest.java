package org.pipelineframework;

import java.util.List;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;
import org.pipelineframework.orchestrator.stream.StreamRegionStatus;
import org.pipelineframework.awaitable.AwaitContinuationStatus;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitStepDescriptor;
import org.pipelineframework.stream.OpaqueSourceCheckpoint;
import org.pipelineframework.stream.ResumableSourceDescriptor;
import org.pipelineframework.stream.StreamRegionAwaitBinding;
import org.pipelineframework.stream.StreamRegionContinuation;
import org.pipelineframework.stream.StreamRegionContinuationRegistry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncSweepFlowTest {

  private QueueAsyncSweepFlow flow;

  @Mock
  private PipelineOrchestratorConfig orchestratorConfig;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private WorkDispatcher workDispatcher;

  @Mock
  private AwaitTimeoutFlow awaitTimeoutFlow;

  @BeforeEach
  void setUp() {
    when(orchestratorConfig.sweepLimit()).thenReturn(100);
    lenient().when(executionStateStore.findDueStreamRegions(anyLong(), anyInt()))
        .thenReturn(Uni.createFrom().item(List.of()));
    flow = new QueueAsyncSweepFlow(
        orchestratorConfig,
        executionStateStore,
        workDispatcher,
        awaitTimeoutFlow);
  }

  @Test
  void sweepRunsTimeoutsThenEnqueuesEveryDueExecution() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(
        record("tenant-b", "exec-b", 20L),
        record("tenant-b", "exec-a", 10L),
        record("tenant-a", "exec-c", 10L))));
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

    flow.sweepOnce(1000L).await().indefinitely();

    InOrder order = inOrder(awaitTimeoutFlow, executionStateStore, workDispatcher);
    order.verify(awaitTimeoutFlow).sweepTimedOut(1000L, 100);
    order.verify(executionStateStore).findDueStreamRegions(1000L, 100);
    order.verify(executionStateStore).findDueExecutions(1000L, 100);
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-a", "exec-c"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-a"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b"));
  }

  @Test
  void sweepEnqueuesDueStreamRegionsUsingTheExistingDispatcher() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.findDueStreamRegions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(
        streamRegion("tenant-b", "exec-b", "source-b", 20L),
        streamRegion("tenant-a", "exec-a", "source-a", 10L))));
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of()));
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

    flow.sweepOnce(1000L).await().indefinitely();

    InOrder order = inOrder(awaitTimeoutFlow, executionStateStore, workDispatcher);
    order.verify(awaitTimeoutFlow).sweepTimedOut(1000L, 100);
    order.verify(executionStateStore).findDueStreamRegions(1000L, 100);
    order.verify(workDispatcher).enqueueNow(ExecutionWorkItem.streamRegion("tenant-b", "exec-b", "source-b"));
    order.verify(workDispatcher).enqueueNow(ExecutionWorkItem.streamRegion("tenant-a", "exec-a", "source-a"));
    order.verify(executionStateStore).findDueExecutions(1000L, 100);
  }

  @Test
  void sweepRedrivesPersistedWaitingStreamInteractionFromPinnedGeneratedBinding() {
    AwaitCoordinator awaitCoordinator = org.mockito.Mockito.mock(AwaitCoordinator.class);
    StreamRegionContinuationRegistry continuations = org.mockito.Mockito.mock(StreamRegionContinuationRegistry.class);
    StreamRegionContinuation continuation = org.mockito.Mockito.mock(StreamRegionContinuation.class);
    AwaitStepDescriptor descriptor = org.mockito.Mockito.mock(AwaitStepDescriptor.class);
    AwaitInteractionRecord interaction = streamInteraction("tenant-a", "exec-a", "source-a", "interaction-a");
    StreamRegionRecord region = streamRegion("tenant-a", "exec-a", "source-a", 1_000L);
    QueueAsyncSweepFlow recoveryFlow = new QueueAsyncSweepFlow(
        orchestratorConfig, executionStateStore, workDispatcher, awaitTimeoutFlow, awaitCoordinator, continuations);
    when(awaitTimeoutFlow.sweepTimedOut(1_000L, 100)).thenReturn(Uni.createFrom().voidItem());
    when(awaitCoordinator.findDueStreamInteractionDispatches(1_000L, 100))
        .thenReturn(Uni.createFrom().item(List.of(interaction)));
    when(awaitCoordinator.findDueItemContinuations(1_000L, 100)).thenReturn(Uni.createFrom().item(List.of()));
    when(executionStateStore.getStreamRegion("tenant-a", "exec-a", "source-a"))
        .thenReturn(Uni.createFrom().item(java.util.Optional.of(region)));
    when(continuations.find(region.source())).thenReturn(java.util.Optional.of(continuation));
    when(continuation.awaitBinding()).thenReturn(new StreamRegionAwaitBinding(descriptor, 3));
    when(awaitCoordinator.dispatch(descriptor, interaction)).thenReturn(Uni.createFrom().item(interaction));
    when(executionStateStore.findDueStreamRegions(1_000L, 100)).thenReturn(Uni.createFrom().item(List.of()));
    when(executionStateStore.findDueExecutions(1_000L, 100)).thenReturn(Uni.createFrom().item(List.of()));

    recoveryFlow.sweepOnce(1_000L).await().indefinitely();

    verify(awaitCoordinator).dispatch(descriptor, interaction);
    verify(workDispatcher, never()).enqueueNow(ExecutionWorkItem.awaitContinuation(
        "tenant-a", "exec-a", "interaction-a"));
  }

  @Test
  void emptyDueBatchDoesNotDispatch() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of()));

    flow.sweepOnce(1000L).await().indefinitely();

    verify(workDispatcher, never()).enqueueNow(any());
  }

  @Test
  void enqueueFailureNamesExecutionId() {
    when(awaitTimeoutFlow.sweepTimedOut(1000L, 100)).thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.findDueExecutions(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(
        record("tenant-a", "exec-a"),
        record("tenant-b", "exec-b"))));
    when(workDispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-a")))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("dispatcher down")));
    when(workDispatcher.enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b")))
        .thenReturn(Uni.createFrom().voidItem());

    IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> flow.sweepOnce(1000L).await().indefinitely());

    assertTrue(error.getMessage().contains("Failed to re-dispatch due execution exec-a"));
    InOrder order = inOrder(workDispatcher);
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-a", "exec-a"));
    order.verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-b", "exec-b"));
  }

  @Test
  void scheduledSweepDoesNotThrowFromCallerWhenSubscriptionFails() {
    when(awaitTimeoutFlow.sweepTimedOut(anyLong(), anyInt()))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("timeout store down")));

    assertDoesNotThrow(() -> flow.sweepDueExecutions());

    verify(awaitTimeoutFlow).sweepTimedOut(anyLong(), anyInt());
  }

  private static ExecutionRecord<Object, Object> record(String tenantId, String executionId) {
    return record(tenantId, executionId, 1L);
  }

  private static ExecutionRecord<Object, Object> record(String tenantId, String executionId, long nextDueEpochMs) {
    return new ExecutionRecord<>(
        tenantId,
        executionId,
        executionId + "-key",
        ExecutionResultShape.SINGLE,
        ExecutionStatus.WAIT_RETRY,
        1L,
        2,
        1,
        null,
        0L,
        nextDueEpochMs,
        null,
        "input",
        null,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }

  private static StreamRegionRecord streamRegion(
      String tenantId,
      String executionId,
      String regionId,
      long nextDueEpochMs
  ) {
    return new StreamRegionRecord(
        tenantId, executionId, regionId,
        new ResumableSourceDescriptor("test", "deterministic", "sha256:source"),
        OpaqueSourceCheckpoint.initial(), 0L, 0, 4, StreamRegionStatus.ACTIVE, java.util.Optional.empty(),
        0L, "", 0L, nextDueEpochMs, 1L, 1L, Long.MAX_VALUE);
  }

  private static AwaitInteractionRecord streamInteraction(
      String tenantId,
      String executionId,
      String regionId,
      String interactionId
  ) {
    return new AwaitInteractionRecord(
        tenantId, executionId, "await-payment-provider", 3, String.class.getName(), interactionId,
        "correlation-" + interactionId, executionId + ":3:0", "idempotency-" + interactionId, 0L,
        AwaitInteractionStatus.WAITING, "request", null, "unit-1", 0, null, null, null, "kafka", java.util.Map.of(),
        70_000L, 1_000L, 1_000L, Long.MAX_VALUE, String.class.getName(), AwaitContinuationStatus.HELD,
        0, 0L, "", 0L, null, regionId);
  }
}
