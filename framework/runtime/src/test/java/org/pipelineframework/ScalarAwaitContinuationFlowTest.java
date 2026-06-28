package org.pipelineframework;

import java.util.Map;
import java.util.Set;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.ControlPlaneProjection;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScalarAwaitContinuationFlowTest {

  @Test
  void scalarResumeRecordsContinuationFactBeforeEnqueue() {
    InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
    ExecutionStateStore store = new InMemoryExecutionStateStore();
    WorkDispatcher dispatcher = Mockito.mock(WorkDispatcher.class);
    long now = 1234L;
    CreateExecutionResult created = store.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-1",
            "key-1",
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.MATERIALIZED_MULTI,
            now,
            9_999_999_999L))
        .await().indefinitely();
    store.markWaitingExternal(
            "tenant-1",
            created.record().executionId(),
            created.record().version(),
            "transition",
            "unit-1",
            2,
            now)
        .await().indefinitely();
    when(dispatcher.enqueueNow(any())).thenAnswer(invocation -> {
      ControlPlaneProjection projection = journal.projection("tenant-1", created.record().executionId())
          .await().indefinitely();
      assertTrue(projection.factKeys().contains("continuation-segment-created:"
          + created.record().executionId() + ":segment:3"));
      return Uni.createFrom().voidItem();
    });
    ScalarAwaitContinuationFlow flow = new ScalarAwaitContinuationFlow(
        store,
        dispatcher,
        () -> new SegmentBoundaryLedger(journal),
        ignored -> {
        });

    flow.release(new AwaitContinuationPlan.ReleaseScalar(
            scalarRecord(created.record().executionId()),
            scalarUnit(created.record().executionId()),
            3),
            now)
        .await().indefinitely();

    verify(dispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", created.record().executionId()));
  }

  private static AwaitInteractionRecord scalarRecord(String executionId) {
    return new AwaitInteractionRecord(
        "tenant-1",
        executionId,
        "AwaitPaymentProvider",
        2,
        String.class.getCanonicalName(),
        "interaction-1",
        "corr-1",
        "cause-1",
        "idem-1",
        1L,
        AwaitInteractionStatus.COMPLETED,
        Map.of("value", "request"),
        Map.of("value", "response"),
        "unit-1",
        null,
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

  private static AwaitUnitRecord scalarUnit(String executionId) {
    return new AwaitUnitRecord(
        "tenant-1",
        "unit-1",
        executionId,
        "AwaitPaymentProvider",
        2,
        "ONE_TO_ONE",
        1L,
        AwaitUnitStatus.COMPLETED,
        "interaction-1",
        null,
        1,
        Set.of(),
        false,
        1L,
        2L,
        999_999L);
  }
}
