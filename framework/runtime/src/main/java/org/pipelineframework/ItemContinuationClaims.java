package org.pipelineframework;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;

class ItemContinuationClaims {

  private final Set<String> dispatchClaims = ConcurrentHashMap.newKeySet();
  private final Map<String, Set<Integer>> completedIndexes = new ConcurrentHashMap<>();

  boolean claimDispatch(ItemContinuationKey key) {
    return dispatchClaims.add(key.dispatchClaimKey());
  }

  void releaseDispatch(ItemContinuationKey key) {
    dispatchClaims.remove(key.dispatchClaimKey());
  }

  boolean recordCompleted(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit,
      int itemIndex) {
    if (unit == null || unit.expectedItemCount() == null) {
      return false;
    }
    Set<Integer> completed = completedIndexes.computeIfAbsent(
        completionKey(parent, unit),
        ignored -> ConcurrentHashMap.newKeySet());
    completed.add(itemIndex);
    return completed.size() >= unit.expectedItemCount();
  }

  void clearDispatches(AwaitUnitRecord unit) {
    if (unit == null) {
      return;
    }
    String prefix = unit.tenantId() + "::"
        + unit.executionId() + "::"
        + unit.unitId() + "::";
    dispatchClaims.removeIf(key -> key.startsWith(prefix));
  }

  void clearCompletions(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit) {
    completedIndexes.remove(completionKey(parent, unit));
  }

  private static String completionKey(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit) {
    return parent.tenantId() + "::" + parent.executionId() + "::" + unit.unitId();
  }
}
