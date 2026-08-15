export function hasAwaitLifecycleCounterEvidence(expectedItemCount, completedItemCount) {
  return expectedItemCount != null || completedItemCount != null;
}

export function awaitOutputCountFromDownstreamStart(lifecycleCounterEvidence, inputItemCount) {
  if (lifecycleCounterEvidence || !Number.isFinite(inputItemCount) || inputItemCount <= 0) {
    return 0;
  }
  return inputItemCount;
}
