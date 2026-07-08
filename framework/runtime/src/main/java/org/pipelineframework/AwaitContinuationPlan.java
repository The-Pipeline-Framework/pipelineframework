package org.pipelineframework;

import java.util.List;

import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;

sealed interface AwaitContinuationPlan
    permits AwaitContinuationPlan.HoldCompletion,
    AwaitContinuationPlan.ReleaseScalar,
    AwaitContinuationPlan.DispatchItemContinuations,
    AwaitContinuationPlan.RecordItemOutput,
    AwaitContinuationPlan.ReleaseItemizedParent,
    AwaitContinuationPlan.FailParent,
    AwaitContinuationPlan.NoOp {

  record HoldCompletion(AwaitInteractionRecord interaction, AwaitUnitRecord unit) implements AwaitContinuationPlan {
    public HoldCompletion {
      if (interaction == null) {
        throw new IllegalArgumentException("Held await completion requires interaction");
      }
      if (unit == null) {
        throw new IllegalArgumentException("Held await completion requires unit");
      }
    }
  }

  record ReleaseScalar(
      AwaitInteractionRecord interaction,
      AwaitUnitRecord unit,
      int nextStepIndex) implements AwaitContinuationPlan {
    public ReleaseScalar {
      if (interaction == null) {
        throw new IllegalArgumentException("Scalar await release requires interaction");
      }
      if (unit == null) {
        throw new IllegalArgumentException("Scalar await release requires unit");
      }
      if (nextStepIndex < 0) {
        throw new IllegalArgumentException("Scalar await release requires a non-negative nextStepIndex");
      }
    }
  }

  record DispatchItemContinuations(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int nextStepIndex) implements AwaitContinuationPlan {
    public DispatchItemContinuations {
      if (parent == null) {
        throw new IllegalArgumentException("Itemized await dispatch requires parent execution");
      }
      if (unit == null) {
        throw new IllegalArgumentException("Itemized await dispatch requires unit");
      }
      if (nextStepIndex < 0) {
        throw new IllegalArgumentException("Itemized await dispatch requires a non-negative nextStepIndex");
      }
      if (!unit.unitId().equals(parent.awaitUnitId())) {
        throw new IllegalArgumentException("Parent await unit does not match itemized dispatch unit");
      }
    }
  }

  record RecordItemOutput(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      AwaitInteractionRecord interaction,
      ItemContinuationKey key,
      int aggregateStepIndex,
      ExecutionInputSnapshot continuationInput,
      List<?> segmentOutputs) implements AwaitContinuationPlan {
    public RecordItemOutput {
      if (parent == null) {
        throw new IllegalArgumentException("Itemized output capture requires parent execution");
      }
      if (unit == null) {
        throw new IllegalArgumentException("Itemized output capture requires unit");
      }
      if (interaction == null) {
        throw new IllegalArgumentException("Itemized output capture requires interaction");
      }
      if (key == null) {
        throw new IllegalArgumentException("Itemized output capture requires key");
      }
      if (aggregateStepIndex < 0) {
        throw new IllegalArgumentException("Aggregate step index must be non-negative: " + aggregateStepIndex);
      }
      if (continuationInput == null) {
        throw new IllegalArgumentException("Itemized await continuation requires normalized continuation input");
      }
      segmentOutputs = segmentOutputs == null ? List.of() : List.copyOf(segmentOutputs);
    }
  }

  record ReleaseItemizedParent(ItemizedParentRelease release) implements AwaitContinuationPlan {
    public ReleaseItemizedParent {
      if (release == null) {
        throw new IllegalArgumentException("Itemized parent release plan requires release");
      }
    }
  }

  record FailParent(AwaitInteractionRecord interaction, Throwable failure, int attempt) implements AwaitContinuationPlan {
    public FailParent {
      if (interaction == null) {
        throw new IllegalArgumentException("Parent failure plan requires interaction");
      }
      if (failure == null) {
        throw new IllegalArgumentException("Parent failure plan requires failure");
      }
      if (attempt < 1) {
        throw new IllegalArgumentException("Parent failure attempt must be positive");
      }
    }
  }

  record NoOp(String reason) implements AwaitContinuationPlan {
    public NoOp {
      reason = reason == null ? "" : reason;
    }
  }
}
