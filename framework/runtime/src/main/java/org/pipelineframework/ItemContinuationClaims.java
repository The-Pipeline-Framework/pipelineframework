package org.pipelineframework;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;

class ItemContinuationClaims {

  private final Set<String> dispatchClaims = ConcurrentHashMap.newKeySet();

  boolean claimDispatch(ItemContinuationKey key) {
    return dispatchClaims.add(key.dispatchClaimKey());
  }

  void releaseDispatch(ItemContinuationKey key) {
    dispatchClaims.remove(key.dispatchClaimKey());
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

  boolean hasPendingClaims(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit) {
    String prefix = unit.tenantId() + "::" + unit.executionId() + "::" + unit.unitId() + "::";
    return dispatchClaims.stream().anyMatch(key -> key.startsWith(prefix));
  }
}
