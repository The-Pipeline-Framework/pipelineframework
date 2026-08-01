package org.pipelineframework;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;

class ItemContinuationClaims {

  private final Set<String> dispatchClaims = ConcurrentHashMap.newKeySet();
  private final Set<String> reconciliationClaims = ConcurrentHashMap.newKeySet();

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
    reconciliationClaims.remove(reconciliationClaimKey(unit.executionId(), unit));
  }

  boolean claimReconciliation(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit) {
    return reconciliationClaims.add(reconciliationClaimKey(parent.executionId(), unit));
  }

  void releaseReconciliation(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit) {
    reconciliationClaims.remove(reconciliationClaimKey(parent.executionId(), unit));
  }

  boolean hasPendingClaims(
      ExecutionRecord<Object, Object> parent,
      AwaitUnitRecord unit) {
    String prefix = unit.tenantId() + "::" + unit.executionId() + "::" + unit.unitId() + "::";
    return dispatchClaims.stream().anyMatch(key -> key.startsWith(prefix));
  }

  private static String reconciliationClaimKey(String executionId, AwaitUnitRecord unit) {
    return unit.tenantId() + "::" + executionId + "::" + unit.unitId();
  }
}
