export function hasAwaitLifecycleCounterEvidence(expectedItemCount, completedItemCount) {
  return expectedItemCount != null || completedItemCount != null;
}

export function isDeferredCompletionStep(step) {
  return step?.deferredCompletion === true || step?.renderRole === "await";
}

export function downstreamReceiptEvidenceKey(targetStep, itemKey) {
  return `${String(targetStep)}\u0000${String(itemKey)}`;
}

export function deferredStartReceiptCount(
  deferredCompletion,
  explicitEmitCounterEvidence,
  targetStep,
  itemKeys,
  inputItemCount
) {
  if (!deferredCompletion || !Number.isFinite(inputItemCount) || inputItemCount <= 0) {
    return 0;
  }
  if (!(explicitEmitCounterEvidence instanceof Set) || !targetStep || !Array.isArray(itemKeys) || itemKeys.length === 0) {
    return inputItemCount;
  }
  const duplicateCount = itemKeys.reduce((count, itemKey) =>
    count + (explicitEmitCounterEvidence.has(downstreamReceiptEvidenceKey(targetStep, itemKey)) ? 1 : 0), 0);
  return Math.max(0, inputItemCount - duplicateCount);
}

export function awaitOutputCountFromDownstreamStart(lifecycleCounterEvidence, inputItemCount) {
  if (lifecycleCounterEvidence || !Number.isFinite(inputItemCount) || inputItemCount <= 0) {
    return 0;
  }
  return inputItemCount;
}
