package org.pipelineframework.command;

import java.util.Map;

import org.pipelineframework.awaitable.AwaitExecutionContext;

/**
 * Connector-facing command execution request.
 */
public record CommandRequest<I>(
    CommandDescriptor descriptor,
    String commandId,
    I input,
    AwaitExecutionContext executionContext,
    Map<String, Object> config
) {
    public CommandRequest {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId must not be blank");
        }
        if (executionContext == null) {
            throw new IllegalArgumentException("executionContext must not be null");
        }
        config = config == null ? Map.of() : Map.copyOf(config);
    }
}
