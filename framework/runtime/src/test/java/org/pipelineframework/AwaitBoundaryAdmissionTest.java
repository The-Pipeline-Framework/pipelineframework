package org.pipelineframework;

import java.util.Map;
import java.util.Set;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.controlplane.ControlPlaneProjection;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwaitBoundaryAdmissionTest {

  private ExecutionInputPolicy inputPolicy;
  private InMemoryControlPlaneJournal journal;
  private SegmentBoundaryLedger ledger;
  private AwaitBoundaryAdmission admission;

  @Mock
  private PipelineOrchestratorConfig orchestratorConfig;

  @Mock
  private AwaitCoordinator awaitCoordinator;

  @Mock
  private AwaitLiveCompletionRegistry liveCompletionRegistry;

  @Mock
  private AwaitContinuations continuations;

  @BeforeEach
  void setUp() {
    inputPolicy = new ExecutionInputPolicy();
    inputPolicy.orchestratorConfig = orchestratorConfig;
    journal = new InMemoryControlPlaneJournal();
    ledger = new SegmentBoundaryLedger(journal);
    ControlPlaneAdmissionPolicy allowAll = ignored -> ControlPlaneAdmissionDecision.allow();
    admission = new AwaitBoundaryAdmission(
        orchestratorConfig,
        inputPolicy,
        allowAll,
        awaitCoordinator,
        liveCompletionRegistry,
        () -> "pipeline-a",
        () -> "release-a",
        () -> ledger,
        continuations);
    when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
  }

  @Test
    void recordsBoundaryCompletionBeforeLiveSignalAndSkipsDurableContinuationWhenAccepted() {
    AwaitInteractionRecord interaction = awaitRecord(null);
    AwaitUnitRecord unit = awaitUnit(AwaitUnitStatus.COMPLETED, null, 0, false, "interaction-1");
    AwaitCompletionCommand command = command(interaction.interactionId());
    when(awaitCoordinator.complete(any()))
        .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(interaction, false)));
    when(awaitCoordinator.recordCompletion(interaction, command.nowEpochMs()))
        .thenReturn(Uni.createFrom().item(unit));
    when(liveCompletionRegistry.signal(interaction, unit))
        .thenAnswer(ignored -> {
          ControlPlaneProjection projection = journal.projection("tenant-1", "exec-1").await().indefinitely();
          assertTrue(projection.factKeys().contains("boundary-completion-admitted:unit-1:idem-1"));
          return Uni.createFrom().item(true);
        });

    AwaitCompletionResult result = admission.complete(command, AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    assertEquals(interaction, result.record());
    verify(liveCompletionRegistry).signal(interaction, unit);
        verify(continuations, never()).afterRecordedCompletion(any(), any(), any(), any(Long.class));
    }

    @Test
    void releasesAdmissionAfterLiveSessionEnqueuesWithoutWaitingForDownstreamDelivery() {
      AwaitInteractionRecord interaction = awaitRecord(null);
      AwaitUnitRecord unit = awaitUnit(AwaitUnitStatus.COMPLETED, null, 0, false, "interaction-1");
      AwaitCompletionCommand command = command(interaction.interactionId());
      when(awaitCoordinator.complete(any()))
          .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(interaction, false)));
      when(awaitCoordinator.recordCompletion(interaction, command.nowEpochMs()))
          .thenReturn(Uni.createFrom().item(unit));
      when(liveCompletionRegistry.signal(interaction, unit)).thenReturn(Uni.createFrom().item(true));
      when(awaitCoordinator.admissionEnabled()).thenReturn(true);
      when(awaitCoordinator.releaseAdmission(interaction)).thenReturn(Uni.createFrom().voidItem());

      admission.complete(command, AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
          .await().indefinitely();

      InOrder order = inOrder(liveCompletionRegistry, awaitCoordinator);
      order.verify(liveCompletionRegistry).signal(interaction, unit);
      order.verify(awaitCoordinator).releaseAdmission(interaction);
      verify(continuations, never()).afterRecordedCompletion(any(), any(), any(), any(Long.class));
    }

  @Test
  void fallsBackToDurableContinuationWhenNoLiveSessionAccepts() {
    AwaitInteractionRecord interaction = awaitRecord(0);
    AwaitUnitRecord unit = awaitUnit(AwaitUnitStatus.COMPLETED, 1, 1, true, null);
    AwaitCompletionCommand command = command(interaction.interactionId());
    AwaitCompletionResult completion = new AwaitCompletionResult(interaction, false);
    when(awaitCoordinator.complete(any())).thenReturn(Uni.createFrom().item(completion));
    when(awaitCoordinator.recordCompletion(interaction, command.nowEpochMs()))
        .thenReturn(Uni.createFrom().item(unit));
    when(liveCompletionRegistry.signal(interaction, unit))
        .thenReturn(Uni.createFrom().item(false));
    when(continuations.afterRecordedCompletion(
            completion,
            unit,
            AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER,
            command.nowEpochMs()))
        .thenReturn(Uni.createFrom().item(completion));

    AwaitCompletionResult result = admission.complete(command, AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER)
        .await().indefinitely();

    assertEquals(interaction, result.record());
    verify(continuations).afterRecordedCompletion(
        completion,
        unit,
        AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER,
        command.nowEpochMs());
  }

  @Test
  void propagatesUnexpectedLiveSignalFailure() {
    AwaitInteractionRecord interaction = awaitRecord(0);
    AwaitUnitRecord unit = awaitUnit(AwaitUnitStatus.COMPLETED, 1, 1, true, null);
    AwaitCompletionCommand command = command(interaction.interactionId());
    AwaitCompletionResult completion = new AwaitCompletionResult(interaction, false);
    when(awaitCoordinator.complete(any())).thenReturn(Uni.createFrom().item(completion));
    when(awaitCoordinator.recordCompletion(interaction, command.nowEpochMs()))
        .thenReturn(Uni.createFrom().item(unit));
    when(liveCompletionRegistry.signal(interaction, unit))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("closed")));

    IllegalStateException error = assertThrows(IllegalStateException.class, () ->
        admission.complete(command, AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER).await().indefinitely());

    assertEquals("closed", error.getMessage());
    verify(continuations, never()).afterRecordedCompletion(any(), any(), any(), any(Long.class));
  }

  @Test
  void preservesDeterministicAdmissionFailure() {
    AwaitCompletionCommand command = command("missing");
    when(awaitCoordinator.complete(any()))
        .thenReturn(Uni.createFrom().failure(new AwaitInteractionNotFoundException("missing")));

    RuntimeException error = assertThrows(RuntimeException.class, () ->
        admission.complete(command, AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER).await().indefinitely());

    assertInstanceOf(AwaitInteractionNotFoundException.class, error);
    verify(awaitCoordinator, never()).recordCompletion(any(), org.mockito.ArgumentMatchers.anyLong());
    verify(liveCompletionRegistry, never()).signal(any(), any());
    verify(continuations, never()).afterRecordedCompletion(any(), any(), any(), any(Long.class));
  }

  private static AwaitCompletionCommand command(String interactionId) {
    return new AwaitCompletionCommand(
        "tenant-1",
        interactionId,
        null,
        null,
        Map.of("value", "approved"),
        "user-1",
        1234L);
  }

  private static AwaitInteractionRecord awaitRecord(Integer itemIndex) {
    return new AwaitInteractionRecord(
        "tenant-1",
        "exec-1",
        "AwaitPaymentProvider",
        2,
        String.class.getCanonicalName(),
        itemIndex == null ? "interaction-1" : "interaction-" + itemIndex,
        "corr-1",
        "cause-1",
        "idem-1",
        1L,
        AwaitInteractionStatus.COMPLETED,
        Map.of("value", "request"),
        Map.of("value", "approved"),
        "unit-1",
        itemIndex,
        "user-1",
        null,
        null,
        "kafka",
        Map.of(),
        10_000L,
        1L,
        2L,
        999_999L);
  }

  private static AwaitUnitRecord awaitUnit(
      AwaitUnitStatus status,
      Integer expectedItemCount,
      int completedItemCount,
      boolean dispatchComplete,
      String primaryInteractionId) {
    return new AwaitUnitRecord(
        "tenant-1",
        "unit-1",
        "exec-1",
        "AwaitPaymentProvider",
        2,
        "ONE_TO_ONE",
        1L,
        status,
        primaryInteractionId,
        expectedItemCount,
        completedItemCount,
        completedItemCount == 0 ? Set.of() : Set.of("item:0"),
        dispatchComplete,
        1L,
        2L,
        999_999L);
  }
}
