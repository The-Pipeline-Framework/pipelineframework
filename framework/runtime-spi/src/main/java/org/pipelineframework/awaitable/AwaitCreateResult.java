package org.pipelineframework.awaitable;

/**
 * Result of create-or-get for an await interaction.
 *
 * @param record interaction record
 * @param duplicate true when an existing active interaction was returned
 */
public record AwaitCreateResult(AwaitInteractionRecord record, boolean duplicate) {
}
