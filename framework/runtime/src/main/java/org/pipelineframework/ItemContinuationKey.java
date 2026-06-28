package org.pipelineframework;

import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;

record ItemContinuationKey(
    String tenantId,
    String parentExecutionId,
    String parentExecutionKey,
    String unitId,
    int itemIndex) {

  ItemContinuationKey {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("Item continuation key requires tenantId");
    }
    if (parentExecutionId == null || parentExecutionId.isBlank()) {
      throw new IllegalArgumentException("Item continuation key requires parentExecutionId");
    }
    if (parentExecutionKey == null || parentExecutionKey.isBlank()) {
      throw new IllegalArgumentException("Item continuation key requires parentExecutionKey");
    }
    if (unitId == null || unitId.isBlank()) {
      throw new IllegalArgumentException("Item continuation key requires unitId");
    }
    if (itemIndex < 0) {
      throw new IllegalArgumentException("Item continuation index must be non-negative: " + itemIndex);
    }
  }

  static ItemContinuationKey from(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      AwaitInteractionRecord interaction) {
    if (interaction == null || interaction.itemIndex() == null) {
      throw new IllegalArgumentException("Itemized await continuation requires itemIndex");
    }
    return from(parent, unit, interaction.itemIndex());
  }

  static ItemContinuationKey from(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int itemIndex) {
    if (parent == null) {
      throw new IllegalArgumentException("Item continuation key requires parent execution");
    }
    if (unit == null) {
      throw new IllegalArgumentException("Item continuation key requires await unit");
    }
    return new ItemContinuationKey(
        parent.tenantId(),
        parent.executionId(),
        parent.executionKey(),
        unit.unitId(),
        itemIndex);
  }

  String childExecutionKey() {
    return parentExecutionKey + ":await-item:" + unitId + ":" + itemIndex;
  }

  String dispatchClaimKey() {
    return tenantId + "::" + parentExecutionId + "::" + unitId + "::" + itemIndex;
  }

  String segmentId() {
    return parentExecutionId + ":segment:await-item:" + unitId + ":" + itemIndex;
  }
}
