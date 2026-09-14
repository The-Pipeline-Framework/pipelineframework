package org.pipelineframework.awaitable;

/**
 * Completion admission result.
 *
 * @param record completed record
 * @param duplicate true when the completion was already accepted earlier
 */
public record AwaitCompletionResult(AwaitInteractionRecord record, boolean duplicate) {
}
