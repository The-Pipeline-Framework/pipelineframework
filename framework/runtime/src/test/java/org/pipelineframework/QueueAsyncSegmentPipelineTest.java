package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.ControlPlaneTransitionAdmission;
import org.pipelineframework.orchestrator.ControlPlaneTransitionPermit;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TransitionWorkerExecutionMode;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.controlplane.TerminalPublicationClaim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncSegmentPipelineTest {

  @Mock
  private PipelineOrchestratorConfig orchestratorConfig;

  @Mock
  private PipelineOrchestratorConfig.WorkerConfig workerConfig;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private WorkDispatcher workDispatcher;

  @Mock
  private DeadLetterPublisher deadLetterPublisher;

  @Mock
  private AwaitCoordinator awaitCoordinator;

  @Mock
  private ExecutionFailureHandler executionFailureHandler;

  @Mock
  private ControlPlaneAdmissionPolicy admissionPolicy;

  @Mock
  private SegmentBoundaryLedger segmentBoundaryLedger;

  @Mock
  private CheckpointPublicationService checkpointPublicationService;

  @Mock
  private ObjectPublishCompletionService objectPublishCompletionService;

  @Mock
  private AwaitContinuations awaitContinuations;

  private JsonTransitionPayloadCodec payloadCodec;
  private TransitionWorkerExecutor transitionWorkerExecutor;

  @BeforeEach
  void setUp() {
    payloadCodec = new JsonTransitionPayloadCodec();
    transitionWorkerExecutor = new TransitionWorkerExecutor(orchestratorConfig, new PipelineInvocationRuntime());
    lenient().when(orchestratorConfig.worker()).thenReturn(workerConfig);
    lenient().when(orchestratorConfig.leaseMs()).thenReturn(1000L);
    lenient().when(workerConfig.executionMode()).thenReturn(TransitionWorkerExecutionMode.SAME_THREAD);
    lenient().when(workerConfig.maxInFlight()).thenReturn(64);
    lenient().when(workerConfig.saturatedDelay()).thenReturn(Duration.ofMillis(25));
    lenient().when(admissionPolicy.admitTransition(any()))
        .thenReturn(ControlPlaneTransitionAdmission.admitted(ControlPlaneTransitionPermit.noop()));
    lenient().when(segmentBoundaryLedger.recordSegmentAttemptStarted(any(), any(), anyLong()))
        .thenReturn(Uni.createFrom().voidItem());
    lenient().when(segmentBoundaryLedger.recordSegmentCompleted(any(), any(), any(), anyLong()))
        .thenReturn(Uni.createFrom().voidItem());
    lenient().when(segmentBoundaryLedger.recordSegmentSuspended(any(), any(), any(), anyLong()))
        .thenReturn(Uni.createFrom().voidItem());
    lenient().when(segmentBoundaryLedger.claimTerminalPublication(
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyString(),
            anyLong()))
        .thenAnswer(invocation -> Uni.createFrom().item(claim(TerminalPublicationClaim.Status.CLAIMED, invocation.getArgument(2))));
    lenient().when(segmentBoundaryLedger.completeTerminalPublication(
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyString(),
            anyLong()))
        .thenReturn(Uni.createFrom().voidItem());
    lenient().when(segmentBoundaryLedger.recordRunSucceeded(any(), any(), anyLong()))
        .thenReturn(Uni.createFrom().voidItem());
    lenient().when(checkpointPublicationService.enabled()).thenReturn(true);
    lenient().when(objectPublishCompletionService.enabled()).thenReturn(true);
    lenient().when(checkpointPublicationService.publishIfConfigured(any(), any()))
        .thenReturn(Uni.createFrom().voidItem());
    lenient().when(objectPublishCompletionService.publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any()))
        .thenReturn(Uni.createFrom().voidItem());
    lenient().when(executionFailureHandler.handleExecutionFailure(any(), any(), any(), any(), any(), any()))
        .thenReturn(Uni.createFrom().voidItem());
    lenient().when(awaitCoordinator.importSuspension(any())).thenReturn(Uni.createFrom().voidItem());
    lenient().when(awaitContinuations.afterParentWaiting(any(), any(), anyLong(), any()))
        .thenReturn(Uni.createFrom().voidItem());
  }

  @Test
  void renewsLeaseWhileAClaimedTransitionIsStillRunning() {
    when(orchestratorConfig.leaseMs()).thenReturn(30L);
    ExecutionRecord<Object, Object> claimed = record("exec-long-running", ExecutionResultShape.SINGLE);
    ExecutionRecord<Object, Object> succeeded = withStatus(claimed, ExecutionStatus.SUCCEEDED, 1L);
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-long-running"), any(), anyLong(), eq(30L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.renewLease(
            eq("tenant-1"), eq("exec-long-running"), eq(0L), any(), anyLong(), eq(30L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.markSucceeded(
            eq("tenant-1"), eq("exec-long-running"), eq(0L), eq("exec-long-running:0:0"),
            eq(List.of("output")), anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded)));

    pipeline(transitionWorkerExecutor, objectPublishCompletionService, () -> 1).process(
            new ExecutionWorkItem("tenant-1", "exec-long-running"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("output")))
                .onItem().delayIt().by(Duration.ofMillis(90)),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    verify(executionStateStore, atLeastOnce()).renewLease(
        eq("tenant-1"), eq("exec-long-running"), eq(0L), any(), anyLong(), eq(30L));
    verify(executionStateStore).markSucceeded(
        eq("tenant-1"), eq("exec-long-running"), eq(0L), eq("exec-long-running:0:0"),
        eq(List.of("output")), anyLong());
  }

  @Test
  void lostLeaseCancelsTheTransitionBeforeItCanCommit() {
    when(orchestratorConfig.leaseMs()).thenReturn(30L);
    ExecutionRecord<Object, Object> claimed = record("exec-lease-lost", ExecutionResultShape.SINGLE);
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-lease-lost"), any(), anyLong(), eq(30L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.renewLease(
            eq("tenant-1"), eq("exec-lease-lost"), eq(0L), any(), anyLong(), eq(30L)))
        .thenReturn(Uni.createFrom().item(Optional.empty()));
    when(executionStateStore.getExecution("tenant-1", "exec-lease-lost"))
        .thenReturn(Uni.createFrom().item(Optional.empty()));

    RuntimeException failure = assertThrows(RuntimeException.class, () -> pipeline(
        transitionWorkerExecutor, objectPublishCompletionService, () -> 1).process(
            new ExecutionWorkItem("tenant-1", "exec-lease-lost"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("output")))
                .onItem().delayIt().by(Duration.ofMillis(90)),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely());

    assertTrue(messageChain(failure).contains("Execution lease ownership was lost"));
    verify(executionStateStore, never()).markSucceeded(any(), any(), anyLong(), any(), any(), anyLong());
  }

  @Test
  void committedTransitionDoesNotLoseToAnOverlappingLeaseRenewal() {
    when(orchestratorConfig.leaseMs()).thenReturn(30L);
    ExecutionRecord<Object, Object> claimed = record("exec-committed-renewal", ExecutionResultShape.SINGLE);
    String transitionKey = "exec-committed-renewal:0:0";
    ExecutionRecord<Object, Object> succeeded = withCommittedTransition(
        claimed, ExecutionStatus.SUCCEEDED, 1L, transitionKey);
    CompletableFuture<Optional<ExecutionRecord<Object, Object>>> renewal = new CompletableFuture<>();
    when(executionStateStore.claimLease(
            eq("tenant-1"), eq("exec-committed-renewal"), any(), anyLong(), eq(30L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.renewLease(
            eq("tenant-1"), eq("exec-committed-renewal"), eq(0L), any(), anyLong(), eq(30L)))
        .thenReturn(Uni.createFrom().completionStage(renewal));
    when(executionStateStore.markSucceeded(
            eq("tenant-1"), eq("exec-committed-renewal"), eq(0L), eq(transitionKey),
            eq(List.of("output")), anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded))
            .onItem().delayIt().by(Duration.ofMillis(25))
            .invoke(() -> renewal.complete(Optional.empty())));
    when(executionStateStore.getExecution("tenant-1", "exec-committed-renewal"))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded)));
    when(segmentBoundaryLedger.recordRunSucceeded(any(), any(), anyLong()))
        .thenReturn(Uni.createFrom().voidItem().onItem().delayIt().by(Duration.ofMillis(25)));

    pipeline(transitionWorkerExecutor, objectPublishCompletionService, () -> 1).process(
            new ExecutionWorkItem("tenant-1", "exec-committed-renewal"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("output"))),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    verify(executionStateStore).getExecution("tenant-1", "exec-committed-renewal");
    verify(segmentBoundaryLedger).recordRunSucceeded(same(succeeded), eq(List.of("output")), anyLong());
  }

  @Test
  void segmentPipelineClaimsRunsPublishesAndCommitsSuccessInOrder() {
    ExecutionRecord<Object, Object> claimed = record("exec-success", ExecutionResultShape.MATERIALIZED_MULTI);
    ExecutionRecord<Object, Object> succeeded = withStatus(claimed, ExecutionStatus.SUCCEEDED, 1L);
    AtomicReference<TransitionCommandEnvelope> commandRef = new AtomicReference<>();
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-success"), any(), anyLong(), eq(1000L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.markSucceeded(
            eq("tenant-1"),
            eq("exec-success"),
            eq(0L),
            eq("exec-success:0:0"),
            eq(List.of("out-1", "out-2")),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded)));

    pipeline().process(
            new ExecutionWorkItem("tenant-1", "exec-success"),
            command -> {
              commandRef.set(command);
              return Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("out-1", "out-2")));
            },
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    assertEquals("exec-success:0:0", commandRef.get().transitionKey());
    assertEquals("input-exec-success", commandRef.get().toCommand(payloadCodec).inputPayload());
    InOrder order = inOrder(
        executionStateStore,
        segmentBoundaryLedger,
        checkpointPublicationService,
        objectPublishCompletionService);
    order.verify(executionStateStore).claimLease(eq("tenant-1"), eq("exec-success"), any(), anyLong(), eq(1000L));
    order.verify(segmentBoundaryLedger).recordSegmentAttemptStarted(same(claimed), eq("exec-success:0:0"), anyLong());
    order.verify(segmentBoundaryLedger).recordSegmentCompleted(same(claimed), eq("exec-success:0:0"), any(), anyLong());
    order.verify(segmentBoundaryLedger).claimTerminalPublication(
        same(claimed),
        eq("exec-success:0:0"),
        eq(TerminalPublicationIntent.CHECKPOINT),
        anyLong());
    order.verify(checkpointPublicationService).publishIfConfigured(same(claimed), eq("out-1"));
    order.verify(segmentBoundaryLedger).completeTerminalPublication(
        same(claimed),
        eq("exec-success:0:0"),
        eq(TerminalPublicationIntent.CHECKPOINT),
        anyLong());
    order.verify(segmentBoundaryLedger).claimTerminalPublication(
        same(claimed),
        eq("exec-success:0:0"),
        eq(TerminalPublicationIntent.OBJECT_PUBLISH),
        anyLong());
    order.verify(objectPublishCompletionService).publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any());
    order.verify(segmentBoundaryLedger).completeTerminalPublication(
        same(claimed),
        eq("exec-success:0:0"),
        eq(TerminalPublicationIntent.OBJECT_PUBLISH),
        anyLong());
    order.verify(executionStateStore).markSucceeded(
        eq("tenant-1"),
        eq("exec-success"),
        eq(0L),
        eq("exec-success:0:0"),
        eq(List.of("out-1", "out-2")),
        anyLong());
    order.verify(segmentBoundaryLedger).recordRunSucceeded(same(succeeded), eq(List.of("out-1", "out-2")), anyLong());
  }

  @Test
  void terminalPublicationFailurePreventsSuccessCommit() {
    ExecutionRecord<Object, Object> claimed = record("exec-publish-fails", ExecutionResultShape.MATERIALIZED_MULTI);
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-publish-fails"), any(), anyLong(), eq(1000L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(objectPublishCompletionService.publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any()))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("publish failed")));

    pipeline().process(
            new ExecutionWorkItem("tenant-1", "exec-publish-fails"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("out"))),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    verify(executionStateStore, never()).markSucceeded(any(), any(), anyLong(), any(), any(), anyLong());
    ArgumentCaptor<Throwable> failureCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(executionFailureHandler).handleExecutionFailure(
        same(claimed),
        eq("exec-publish-fails:0:0"),
        failureCaptor.capture(),
        same(executionStateStore),
        same(workDispatcher),
        same(deadLetterPublisher));
    assertEquals("publish failed", failureCaptor.getValue().getMessage());
  }

  @Test
  void terminalMaterializedSegmentStaysAtCoordinatorWithoutEncodingOrWorkerDispatch() {
    ExecutionRecord<Object, Object> claimed = withCurrentStep(
        record("exec-terminal-pass-through", ExecutionResultShape.MATERIALIZED_MULTI),
        1,
        List.of("out-1", "out-2"));
    ExecutionRecord<Object, Object> succeeded = withStatus(claimed, ExecutionStatus.SUCCEEDED, 1L);
    payloadCodec = org.mockito.Mockito.spy(payloadCodec);
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-terminal-pass-through"), any(), anyLong(), eq(1000L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.markSucceeded(
            eq("tenant-1"),
            eq("exec-terminal-pass-through"),
            eq(0L),
            eq("exec-terminal-pass-through:1:0"),
            eq(List.of("out-1", "out-2")),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded)));

    pipeline(transitionWorkerExecutor, objectPublishCompletionService, () -> 1).process(
            new ExecutionWorkItem("tenant-1", "exec-terminal-pass-through"),
            command -> Uni.createFrom().failure(new AssertionError("terminal segment must not call worker")),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    verify(payloadCodec, never()).encode(any());
    verify(executionStateStore).markSucceeded(
        eq("tenant-1"),
        eq("exec-terminal-pass-through"),
        eq(0L),
        eq("exec-terminal-pass-through:1:0"),
        eq(List.of("out-1", "out-2")),
        anyLong());
  }

  @Test
  void terminalAwaitResumeLoadsCanonicalAwaitOutputBeforeWorkerDispatch() {
    ExecutionRecord<Object, Object> claimed = withAwaitUnitId(
        withCurrentStep(record("exec-terminal-await", ExecutionResultShape.SINGLE), 1, "stale-input"),
        "unit-terminal-await");
    ExecutionRecord<Object, Object> succeeded = withStatus(claimed, ExecutionStatus.SUCCEEDED, 1L);
    AtomicReference<TransitionCommandEnvelope> commandRef = new AtomicReference<>();
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-terminal-await"), any(), anyLong(), eq(1000L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(awaitCoordinator.loadResumePayload(eq("tenant-1"), eq("unit-terminal-await")))
        .thenReturn(Uni.createFrom().item("canonical-await-output"));
    when(executionStateStore.markSucceeded(
            eq("tenant-1"),
            eq("exec-terminal-await"),
            eq(0L),
            eq("exec-terminal-await:1:0"),
            eq("canonical-await-output"),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded)));

    pipeline(transitionWorkerExecutor, objectPublishCompletionService, () -> 1).process(
            new ExecutionWorkItem("tenant-1", "exec-terminal-await"),
            command -> {
              commandRef.set(command);
              return Uni.createFrom().item(
                  TransitionResultEnvelope.completedInProcess(List.of("canonical-await-output")));
            },
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    verify(awaitCoordinator).loadResumePayload("tenant-1", "unit-terminal-await");
    assertEquals("canonical-await-output", commandRef.get().toCommand(payloadCodec).inputPayload());
  }

  @Test
  void ordinarySegmentDoesNotResolveTerminalStepCount() {
    ExecutionRecord<Object, Object> claimed = record("exec-ordinary", ExecutionResultShape.SINGLE);
    ExecutionRecord<Object, Object> succeeded = withStatus(claimed, ExecutionStatus.SUCCEEDED, 1L);
    java.util.concurrent.atomic.AtomicInteger resolutions = new java.util.concurrent.atomic.AtomicInteger();
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-ordinary"), any(), anyLong(), eq(1000L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.markSucceeded(
            eq("tenant-1"),
            eq("exec-ordinary"),
            eq(0L),
            eq("exec-ordinary:0:0"),
            eq("output"),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(succeeded)));

    pipeline(transitionWorkerExecutor, objectPublishCompletionService, () -> {
      resolutions.incrementAndGet();
      return 1;
    }).process(
            new ExecutionWorkItem("tenant-1", "exec-ordinary"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("output"))),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    assertEquals(0, resolutions.get());
  }

  @Test
  void awaitSuspensionImportsMarksWaitingAndReleasesCompletedWork() {
    ExecutionRecord<Object, Object> claimed = record("exec-await", ExecutionResultShape.MATERIALIZED_MULTI);
    ExecutionRecord<Object, Object> waiting = withStatus(claimed, ExecutionStatus.WAITING_EXTERNAL, 1L);
    TransitionAwaitSuspension suspension = new TransitionAwaitSuspension("tenant-1", "exec-await", "unit-1", 2);
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-await"), any(), anyLong(), eq(1000L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.markWaitingExternal(
            eq("tenant-1"),
            eq("exec-await"),
            eq(0L),
            eq("exec-await:0:0"),
            eq("unit-1"),
            eq(2),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(waiting)));

    pipeline().process(
            new ExecutionWorkItem("tenant-1", "exec-await"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.waiting(suspension)),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    InOrder order = inOrder(executionStateStore, awaitCoordinator, segmentBoundaryLedger, awaitContinuations);
    order.verify(executionStateStore).markWaitingExternal(
        eq("tenant-1"),
        eq("exec-await"),
        eq(0L),
        eq("exec-await:0:0"),
        eq("unit-1"),
        eq(2),
        anyLong());
    order.verify(awaitCoordinator).importSuspension(same(suspension));
    order.verify(segmentBoundaryLedger).recordSegmentSuspended(same(claimed), eq("exec-await:0:0"), same(suspension), anyLong());
    order.verify(awaitContinuations).afterParentWaiting(
        same(waiting),
        same(suspension),
        anyLong(),
        same(AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER));
  }

  @Test
  void missedSuccessCasFailsCommitAndSkipsRunSucceededFact() {
    ExecutionRecord<Object, Object> claimed = record("exec-success-cas-miss", ExecutionResultShape.MATERIALIZED_MULTI);
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-success-cas-miss"), any(), anyLong(), eq(1000L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
    when(executionStateStore.markSucceeded(
            eq("tenant-1"),
            eq("exec-success-cas-miss"),
            eq(0L),
            eq("exec-success-cas-miss:0:0"),
            eq(List.of("out")),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.empty()));

    pipeline().process(
            new ExecutionWorkItem("tenant-1", "exec-success-cas-miss"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("out"))),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    ArgumentCaptor<Throwable> failureCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(executionFailureHandler).handleExecutionFailure(
        same(claimed),
        eq("exec-success-cas-miss:0:0"),
        failureCaptor.capture(),
        same(executionStateStore),
        same(workDispatcher),
        same(deadLetterPublisher));
    assertTrue(failureCaptor.getValue().getMessage().contains("Failed to persist SUCCEEDED state"));
    verify(segmentBoundaryLedger, never()).recordRunSucceeded(any(), any(), anyLong());
  }

  @Test
  void workerFailureDelegatesToFailureHandler() {
    ExecutionRecord<Object, Object> claimed = record("exec-failed", ExecutionResultShape.SINGLE);
    when(executionStateStore.claimLease(eq("tenant-1"), eq("exec-failed"), any(), anyLong(), eq(1000L)))
        .thenReturn(Uni.createFrom().item(Optional.of(claimed)));

    pipeline().process(
            new ExecutionWorkItem("tenant-1", "exec-failed"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.failed(new IllegalArgumentException("bad item"))),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    ArgumentCaptor<Throwable> failureCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(executionFailureHandler).handleExecutionFailure(
        same(claimed),
        eq("exec-failed:0:0"),
        failureCaptor.capture(),
        same(executionStateStore),
        same(workDispatcher),
        same(deadLetterPublisher));
    assertTrue(failureCaptor.getValue().getMessage().contains("bad item"));
  }

  @Test
  void workerAdmissionSaturationRequeuesWithoutClaiming() {
    TransitionWorkerExecutor saturatedExecutor = new TransitionWorkerExecutor(orchestratorConfig, new PipelineInvocationRuntime());
    when(workerConfig.maxInFlight()).thenReturn(1);
    when(workDispatcher.enqueueDelayed(any(), eq(Duration.ofMillis(25))))
        .thenReturn(Uni.createFrom().voidItem());
    TransitionWorkerExecutor.TransitionAdmission held = saturatedExecutor.tryAdmit().orElseThrow();
    try {
      pipeline(saturatedExecutor, objectPublishCompletionService).process(
              new ExecutionWorkItem("tenant-1", "exec-saturated"),
              command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of())),
              AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
          .await().indefinitely();
    } finally {
      held.close();
    }

    verify(executionStateStore, never()).claimLease(any(), any(), any(), anyLong(), anyLong());
    verify(workDispatcher).enqueueDelayed(
        eq(new ExecutionWorkItem("tenant-1", "exec-saturated")),
        eq(Duration.ofMillis(25)));
  }

  @Test
  void tenantAdmissionSaturationClosesWorkerPermitAndRequeuesWithoutClaiming() {
    when(workDispatcher.enqueueDelayed(any(), eq(Duration.ofMillis(25))))
        .thenReturn(Uni.createFrom().voidItem());
    when(admissionPolicy.admitTransition(any()))
        .thenReturn(ControlPlaneTransitionAdmission.denied(ControlPlaneAdmissionDecision.deny(
            ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED,
            "tenant saturated")));

    pipeline().process(
            new ExecutionWorkItem("tenant-1", "exec-tenant-saturated"),
            command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of())),
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    verify(executionStateStore, never()).claimLease(any(), any(), any(), anyLong(), anyLong());
    verify(workDispatcher).enqueueDelayed(
        eq(new ExecutionWorkItem("tenant-1", "exec-tenant-saturated")),
        eq(Duration.ofMillis(25)));
    assertEquals(0, transitionWorkerExecutor.activePermits());
  }

  private QueueAsyncSegmentPipeline pipeline() {
    return pipeline(transitionWorkerExecutor, objectPublishCompletionService);
  }

  private QueueAsyncSegmentPipeline pipeline(
      TransitionWorkerExecutor executor,
      ObjectPublishCompletionService publishCompletionService) {
    return pipeline(executor, publishCompletionService, () -> 1);
  }

  private QueueAsyncSegmentPipeline pipeline(
      TransitionWorkerExecutor executor,
      ObjectPublishCompletionService publishCompletionService,
      IntSupplier pipelineStepCount) {
    return new QueueAsyncSegmentPipeline(
        orchestratorConfig,
        executionStateStore,
        workDispatcher,
        awaitCoordinator,
        executor,
        admissionPolicy,
        () -> payloadCodec,
        () -> segmentBoundaryLedger,
        () -> Duration.ofMillis(25),
        pipelineStepCount,
        new SegmentCommitEffects(
            executionStateStore,
            workDispatcher,
            deadLetterPublisher,
            awaitCoordinator,
            executionFailureHandler,
            () -> segmentBoundaryLedger,
            awaitContinuations,
            new TerminalPublicationBoundary(
                checkpointPublicationService,
                publishCompletionService,
                () -> payloadCodec,
                () -> segmentBoundaryLedger),
            ignored -> {
            }),
        "worker-test");
  }

  private static ExecutionRecord<Object, Object> withCurrentStep(
      ExecutionRecord<Object, Object> source,
      int currentStepIndex,
      Object inputPayload) {
    return new ExecutionRecord<>(
        source.tenantId(),
        source.executionId(),
        source.executionKey(),
        source.pipelineId(),
        source.contractVersion(),
        source.releaseVersion(),
        source.resultShape(),
        source.status(),
        source.version(),
        currentStepIndex,
        source.attempt(),
        source.leaseOwner(),
        source.leaseExpiresEpochMs(),
        source.nextDueEpochMs(),
        source.lastTransitionKey(),
        inputPayload,
        source.awaitUnitId(),
        source.resultPayload(),
        source.errorCode(),
        source.errorMessage(),
        source.createdAtEpochMs(),
        source.updatedAtEpochMs(),
        source.ttlEpochS(),
        source.firstCircuitDeferredAtEpochMs(),
        source.circuitDeferralCount(),
        source.circuitIdentity());
  }

  private static ExecutionRecord<Object, Object> withAwaitUnitId(
      ExecutionRecord<Object, Object> source,
      String awaitUnitId) {
    return new ExecutionRecord<>(
        source.tenantId(),
        source.executionId(),
        source.executionKey(),
        source.pipelineId(),
        source.contractVersion(),
        source.releaseVersion(),
        source.resultShape(),
        source.status(),
        source.version(),
        source.currentStepIndex(),
        source.attempt(),
        source.leaseOwner(),
        source.leaseExpiresEpochMs(),
        source.nextDueEpochMs(),
        source.lastTransitionKey(),
        source.inputPayload(),
        awaitUnitId,
        source.resultPayload(),
        source.errorCode(),
        source.errorMessage(),
        source.createdAtEpochMs(),
        source.updatedAtEpochMs(),
        source.ttlEpochS(),
        source.firstCircuitDeferredAtEpochMs(),
        source.circuitDeferralCount(),
        source.circuitIdentity());
  }

  private static ExecutionRecord<Object, Object> record(String executionId, ExecutionResultShape resultShape) {
    return new ExecutionRecord<>(
        "tenant-1",
        executionId,
        "key-" + executionId,
        "pipeline-a",
        "contract-a",
        "release-a",
        resultShape,
        ExecutionStatus.QUEUED,
        0L,
        0,
        0,
        null,
        0L,
        0L,
        null,
        "input-" + executionId,
        null,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }

  private static ExecutionRecord<Object, Object> withStatus(
      ExecutionRecord<Object, Object> source,
      ExecutionStatus status,
      long version) {
    return new ExecutionRecord<>(
        source.tenantId(),
        source.executionId(),
        source.executionKey(),
        source.pipelineId(),
        source.contractVersion(),
        source.releaseVersion(),
        source.resultShape(),
        status,
        version,
        source.currentStepIndex(),
        source.attempt(),
        source.leaseOwner(),
        source.leaseExpiresEpochMs(),
        source.nextDueEpochMs(),
        source.lastTransitionKey(),
        source.inputPayload(),
        source.awaitUnitId(),
        source.resultPayload(),
        source.errorCode(),
        source.errorMessage(),
        source.createdAtEpochMs(),
        source.updatedAtEpochMs(),
        source.ttlEpochS());
  }

  private static ExecutionRecord<Object, Object> withCommittedTransition(
      ExecutionRecord<Object, Object> source,
      ExecutionStatus status,
      long version,
      String transitionKey) {
    return new ExecutionRecord<>(
        source.tenantId(),
        source.executionId(),
        source.executionKey(),
        source.pipelineId(),
        source.contractVersion(),
        source.releaseVersion(),
        source.resultShape(),
        status,
        version,
        source.currentStepIndex(),
        source.attempt(),
        source.leaseOwner(),
        source.leaseExpiresEpochMs(),
        source.nextDueEpochMs(),
        transitionKey,
        source.inputPayload(),
        source.awaitUnitId(),
        source.resultPayload(),
        source.errorCode(),
        source.errorMessage(),
        source.createdAtEpochMs(),
        source.updatedAtEpochMs(),
        source.ttlEpochS());
  }

  private static TerminalPublicationClaim claim(TerminalPublicationClaim.Status status, String kind) {
    return new TerminalPublicationClaim(
        status,
        "publication-" + kind,
        "idempotency-" + kind,
        "terminal-publication-prepared:publication-" + kind + ":idempotency-" + kind,
        "terminal-publication-completed:publication-" + kind + ":idempotency-" + kind);
  }

  private static String messageChain(Throwable failure) {
    StringBuilder messages = new StringBuilder();
    Throwable current = failure;
    while (current != null) {
      if (current.getMessage() != null) {
        messages.append(current.getMessage()).append(' ');
      }
      current = current.getCause();
    }
    return messages.toString();
  }
}
