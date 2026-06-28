package org.pipelineframework;

import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;

record ItemizedParentRelease(
    ExecutionRecord<Object, Object> parent,
    AwaitUnitRecord unit,
    int aggregateStepIndex,
    ExecutionInputSnapshot resumePayload) {

  ItemizedParentRelease {
    if (parent == null) {
      throw new IllegalArgumentException("Itemized parent release requires parent execution");
    }
    if (unit == null) {
      throw new IllegalArgumentException("Itemized parent release requires await unit");
    }
    if (aggregateStepIndex < 0) {
      throw new IllegalArgumentException("Aggregate step index must be non-negative: " + aggregateStepIndex);
    }
    if (resumePayload == null) {
      throw new IllegalArgumentException("Itemized parent release requires resume payload");
    }
  }

  String segmentId() {
    return parent.executionId() + ":segment:" + aggregateStepIndex;
  }
}
