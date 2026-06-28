package org.pipelineframework;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;

class AwaitContinuationPlanner {

  static final String ONE_TO_ONE_CARDINALITY = "ONE_TO_ONE";

  AwaitContinuationPlan afterRecordedCompletion(
      AwaitInteractionRecord interaction,
      AwaitUnitRecord unit,
      Optional<ExecutionRecord<Object, Object>> parent) {
    if (!usesItemContinuations(interaction, unit)) {
      return unit != null && unit.status() == AwaitUnitStatus.COMPLETED
          ? new AwaitContinuationPlan.ReleaseScalar(interaction, unit, interaction.stepIndex() + 1)
          : new AwaitContinuationPlan.NoOp("scalar-await-unit-not-complete");
    }
    if (!itemContinuationReady(parent, unit)) {
      return new AwaitContinuationPlan.HoldCompletion(interaction, unit);
    }
    return new AwaitContinuationPlan.DispatchItemContinuations(parent.orElseThrow(), unit, interaction.stepIndex() + 1);
  }

  AwaitContinuationPlan afterParentWaiting(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int suspendedStepIndex) {
    if (unit == null) {
      return new AwaitContinuationPlan.NoOp("await-unit-missing");
    }
    if (usesItemContinuations(unit)) {
      if (!itemContinuationReady(Optional.ofNullable(parent), unit)) {
        return new AwaitContinuationPlan.NoOp("itemized-parent-not-ready");
      }
      return new AwaitContinuationPlan.DispatchItemContinuations(parent, unit, suspendedStepIndex + 1);
    }
    if (unit.status() != AwaitUnitStatus.COMPLETED) {
      return new AwaitContinuationPlan.NoOp("scalar-await-unit-not-complete");
    }
    return new AwaitContinuationPlan.ReleaseScalar(
        scalarInteraction(parent, unit, suspendedStepIndex),
        unit,
        suspendedStepIndex + 1);
  }

  AwaitContinuationPlan recordItemOutput(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      AwaitInteractionRecord interaction,
      int aggregateStepIndex,
      ExecutionInputSnapshot continuationInput,
      List<?> segmentOutputs) {
    if (!usesItemContinuations(interaction, unit)) {
      return new AwaitContinuationPlan.NoOp("not-itemized-await-continuation");
    }
    validateItemIndex(interaction, unit);
    if (continuationInput == null) {
      throw new IllegalArgumentException("Itemized await continuation requires normalized continuation input");
    }
    return new AwaitContinuationPlan.RecordItemOutput(
        parent,
        unit,
        interaction,
        ItemContinuationKey.from(parent, unit, interaction),
        aggregateStepIndex,
        copyInputSnapshot(continuationInput),
        segmentOutputs);
  }

  AwaitContinuationPlan releaseItemizedParent(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int aggregateStepIndex,
      List<Optional<ExecutionRecord<Object, Object>>> children) {
    if (parent == null || unit == null || !usesItemContinuations(unit)
        || !unit.dispatchComplete() || unit.expectedItemCount() == null) {
      return new AwaitContinuationPlan.NoOp("itemized-parent-release-not-ready");
    }
    List<Object> orderedOutputs = new ArrayList<>();
    for (Optional<ExecutionRecord<Object, Object>> child : children) {
      if (child.isEmpty() || child.get().status() != ExecutionStatus.SUCCEEDED) {
        return new AwaitContinuationPlan.NoOp("itemized-child-not-succeeded");
      }
      Object payload = child.get().resultPayload();
      if (payload instanceof Iterable<?> iterable) {
        iterable.forEach(orderedOutputs::add);
      } else if (payload != null) {
        orderedOutputs.add(payload);
      }
    }
    return new AwaitContinuationPlan.ReleaseItemizedParent(new ItemizedParentRelease(
        parent,
        unit,
        aggregateStepIndex,
        new ExecutionInputSnapshot(ExecutionInputShape.MULTI, List.copyOf(orderedOutputs))));
  }

  void validateItemIndex(AwaitInteractionRecord interaction, AwaitUnitRecord unit) {
    if (interaction == null || interaction.itemIndex() == null) {
      throw new IllegalArgumentException("Itemized await continuation requires itemIndex");
    }
    int itemIndex = interaction.itemIndex();
    if (itemIndex < 0) {
      throw new IllegalArgumentException(
          "Itemized await continuation itemIndex must be non-negative: " + itemIndex);
    }
    Integer expectedItemCount = unit == null ? null : unit.expectedItemCount();
    if (expectedItemCount != null && itemIndex >= expectedItemCount) {
      throw new IllegalArgumentException(
          "Itemized await continuation itemIndex " + itemIndex
              + " is outside expected item count " + expectedItemCount);
    }
  }

  boolean itemContinuationReady(
      Optional<ExecutionRecord<Object, Object>> parent,
      AwaitUnitRecord unit) {
    return parent.isPresent()
        && parent.get().status() == ExecutionStatus.WAITING_EXTERNAL
        && unit != null
        && unit.dispatchComplete()
        && unit.unitId().equals(parent.get().awaitUnitId());
  }

  static boolean usesItemContinuations(
      AwaitInteractionRecord interaction,
      AwaitUnitRecord unit) {
    return interaction != null
        && interaction.itemInteraction()
        && usesItemContinuations(unit);
  }

  static boolean usesItemContinuations(AwaitUnitRecord unit) {
    return unit != null
        && unit.primaryInteractionId() == null
        && ONE_TO_ONE_CARDINALITY.equalsIgnoreCase(unit.cardinality());
  }

  private static AwaitInteractionRecord scalarInteraction(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int suspendedStepIndex) {
    return new AwaitInteractionRecord(
        parent == null ? unit.tenantId() : parent.tenantId(),
        parent == null ? unit.executionId() : parent.executionId(),
        unit.stepId(),
        suspendedStepIndex,
        Object.class.getCanonicalName(),
        unit.primaryInteractionId() == null ? "scalar:" + unit.unitId() : unit.primaryInteractionId(),
        "scalar:" + unit.unitId(),
        "await-unit:" + unit.unitId(),
        "scalar:" + unit.unitId(),
        1L,
        AwaitInteractionStatus.COMPLETED,
        null,
        null,
        unit.unitId(),
        null,
        null,
        null,
        null,
        null,
        java.util.Map.of(),
        unit.ttlEpochS(),
        parent == null ? unit.createdAtEpochMs() : parent.createdAtEpochMs(),
        unit.updatedAtEpochMs(),
        unit.ttlEpochS());
  }

  private static ExecutionInputSnapshot copyInputSnapshot(ExecutionInputSnapshot snapshot) {
    Object payload = snapshot.payload();
    if (payload instanceof List<?> list) {
      payload = List.copyOf(list);
    }
    return new ExecutionInputSnapshot(snapshot.shape(), payload);
  }
}
