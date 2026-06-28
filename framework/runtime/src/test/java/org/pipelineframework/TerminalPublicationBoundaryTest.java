package org.pipelineframework;

import java.util.List;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.controlplane.TerminalPublicationClaim;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TerminalPublicationBoundaryTest {

  @Mock
  private CheckpointPublicationService checkpointPublicationService;

  @Mock
  private ObjectPublishCompletionService objectPublishCompletionService;

  @Mock
  private SegmentBoundaryLedger segmentBoundaryLedger;

  private JsonTransitionPayloadCodec payloadCodec;
  private ClaimedSegment segment;
  private CompletedSegment completed;
  private ExecutionRecord<Object, Object> record;

  @BeforeEach
  void setUp() {
    payloadCodec = new JsonTransitionPayloadCodec();
    record = record();
    segment = ClaimedSegment.from(record);
    TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1", "out-2"));
    completed = (CompletedSegment) SegmentCommitPlan.from(segment, result);
    org.mockito.Mockito.lenient().when(checkpointPublicationService.enabled()).thenReturn(true);
    org.mockito.Mockito.lenient().when(objectPublishCompletionService.enabled()).thenReturn(true);
  }

  @Test
  void publicationClaimsBeforeEffectsAndCompletesAfterEffects() {
    long now = 100L;
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.CHECKPOINT, now))
        .thenReturn(Uni.createFrom().item(claim(TerminalPublicationClaim.Status.CLAIMED, TerminalPublicationIntent.CHECKPOINT)));
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.OBJECT_PUBLISH, now))
        .thenReturn(Uni.createFrom().item(claim(TerminalPublicationClaim.Status.CLAIMED, TerminalPublicationIntent.OBJECT_PUBLISH)));
    when(segmentBoundaryLedger.completeTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.CHECKPOINT, now))
        .thenReturn(Uni.createFrom().voidItem());
    when(segmentBoundaryLedger.completeTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.OBJECT_PUBLISH, now))
        .thenReturn(Uni.createFrom().voidItem());
    when(checkpointPublicationService.publishIfConfigured(record, "out-1")).thenReturn(Uni.createFrom().voidItem());
    when(objectPublishCompletionService.publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any()))
        .thenReturn(Uni.createFrom().voidItem());

    boundary().publishBeforeSuccess(completed, now).await().indefinitely();

    InOrder order = inOrder(segmentBoundaryLedger, checkpointPublicationService, objectPublishCompletionService);
    order.verify(segmentBoundaryLedger).claimTerminalPublication(
        record,
        segment.transitionKey(),
        TerminalPublicationIntent.CHECKPOINT,
        now);
    order.verify(checkpointPublicationService).publishIfConfigured(record, "out-1");
    order.verify(segmentBoundaryLedger).completeTerminalPublication(
        record,
        segment.transitionKey(),
        TerminalPublicationIntent.CHECKPOINT,
        now);
    order.verify(segmentBoundaryLedger).claimTerminalPublication(
        record,
        segment.transitionKey(),
        TerminalPublicationIntent.OBJECT_PUBLISH,
        now);
    order.verify(objectPublishCompletionService).publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any());
    order.verify(segmentBoundaryLedger).completeTerminalPublication(
        record,
        segment.transitionKey(),
        TerminalPublicationIntent.OBJECT_PUBLISH,
        now);
  }

  @Test
  void alreadyCompletedPublicationSkipsExternalEffects() {
    long now = 101L;
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.CHECKPOINT, now))
        .thenReturn(Uni.createFrom().item(claim(
            TerminalPublicationClaim.Status.ALREADY_COMPLETED,
            TerminalPublicationIntent.CHECKPOINT)));
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.OBJECT_PUBLISH, now))
        .thenReturn(Uni.createFrom().item(claim(
            TerminalPublicationClaim.Status.ALREADY_COMPLETED,
            TerminalPublicationIntent.OBJECT_PUBLISH)));

    boundary().publishBeforeSuccess(completed, now).await().indefinitely();

    verify(checkpointPublicationService, never()).publishIfConfigured(same(record), eq("out-1"));
    verify(objectPublishCompletionService, never())
        .publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any());
    verify(segmentBoundaryLedger, never()).completeTerminalPublication(anyRecord(), eq(segment.transitionKey()), eq(TerminalPublicationIntent.CHECKPOINT), anyLong());
    verify(segmentBoundaryLedger, never()).completeTerminalPublication(anyRecord(), eq(segment.transitionKey()), eq(TerminalPublicationIntent.OBJECT_PUBLISH), anyLong());
  }

  @Test
  void preparedRetryRunsEffectAndCompletesPublication() {
    long now = 102L;
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.CHECKPOINT, now))
        .thenReturn(Uni.createFrom().item(claim(
            TerminalPublicationClaim.Status.ALREADY_COMPLETED,
            TerminalPublicationIntent.CHECKPOINT)));
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.OBJECT_PUBLISH, now))
        .thenReturn(Uni.createFrom().item(claim(
            TerminalPublicationClaim.Status.PREPARED_RETRY,
            TerminalPublicationIntent.OBJECT_PUBLISH)));
    when(segmentBoundaryLedger.completeTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.OBJECT_PUBLISH, now))
        .thenReturn(Uni.createFrom().voidItem());
    when(objectPublishCompletionService.publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any()))
        .thenReturn(Uni.createFrom().voidItem());

    boundary().publishBeforeSuccess(completed, now).await().indefinitely();

    verify(objectPublishCompletionService).publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any());
    verify(segmentBoundaryLedger).completeTerminalPublication(
        record,
        segment.transitionKey(),
        TerminalPublicationIntent.OBJECT_PUBLISH,
        now);
  }

  @Test
  void claimFailurePreventsPublication() {
    long now = 103L;
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.CHECKPOINT, now))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("claim failed")));

    assertThrows(IllegalStateException.class, () -> boundary().publishBeforeSuccess(completed, now).await().indefinitely());

    verify(checkpointPublicationService, never()).publishIfConfigured(same(record), eq("out-1"));
    verify(objectPublishCompletionService, never())
        .publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any());
  }

  @Test
  void publicationFailureDoesNotCompletePublication() {
    long now = 104L;
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.CHECKPOINT, now))
        .thenReturn(Uni.createFrom().item(claim(
            TerminalPublicationClaim.Status.ALREADY_COMPLETED,
            TerminalPublicationIntent.CHECKPOINT)));
    when(segmentBoundaryLedger.claimTerminalPublication(record, segment.transitionKey(), TerminalPublicationIntent.OBJECT_PUBLISH, now))
        .thenReturn(Uni.createFrom().item(claim(TerminalPublicationClaim.Status.CLAIMED, TerminalPublicationIntent.OBJECT_PUBLISH)));
    when(objectPublishCompletionService.publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any()))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("publish failed")));

    assertThrows(IllegalStateException.class, () -> boundary().publishBeforeSuccess(completed, now).await().indefinitely());

    verify(segmentBoundaryLedger, never()).completeTerminalPublication(
        record,
        segment.transitionKey(),
        TerminalPublicationIntent.OBJECT_PUBLISH,
        now);
  }

  private TerminalPublicationBoundary boundary() {
    return new TerminalPublicationBoundary(
        checkpointPublicationService,
        objectPublishCompletionService,
        () -> payloadCodec,
        () -> segmentBoundaryLedger);
  }

  private static TerminalPublicationClaim claim(TerminalPublicationClaim.Status status, String kind) {
    return new TerminalPublicationClaim(
        status,
        "exec-1:terminal-output:" + kind,
        "exec-1:terminal-output:" + kind + ":terminal-publication",
        "terminal-publication-prepared:exec-1:terminal-output:" + kind + ":exec-1:terminal-output:" + kind + ":terminal-publication",
        "terminal-publication-completed:exec-1:terminal-output:" + kind + ":exec-1:terminal-output:" + kind + ":terminal-publication");
  }

  @SuppressWarnings("unchecked")
  private static ExecutionRecord<Object, Object> anyRecord() {
    return org.mockito.ArgumentMatchers.any(ExecutionRecord.class);
  }

  private static ExecutionRecord<Object, Object> record() {
    return new ExecutionRecord<>(
        "tenant-1",
        "exec-1",
        "key-1",
        "pipeline-a",
        "contract-a",
        "release-a",
        ExecutionResultShape.MATERIALIZED_MULTI,
        ExecutionStatus.QUEUED,
        0L,
        0,
        0,
        null,
        0L,
        0L,
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
}
