package org.pipelineframework.command;

import java.util.Map;

import org.pipelineframework.execution.PipelineExecutionContext;

/**
 * Connector-facing command execution request.
 */
public record CommandRequest<I>(
    CommandDescriptor descriptor,
    String commandId,
    String occurrenceId,
    String attemptId,
    I input,
    PipelineExecutionContext executionContext,
    Map<String, Object> config
) {
    public CommandRequest(
        CommandDescriptor descriptor,
        String commandId,
        I input,
        PipelineExecutionContext executionContext,
        Map<String, Object> config
    ) {
        this(descriptor, commandId, commandId, newAttemptId(), input, executionContext, config);
    }

    public CommandRequest(
        CommandDescriptor descriptor,
        String commandId,
        String attemptId,
        I input,
        PipelineExecutionContext executionContext,
        Map<String, Object> config
    ) {
        this(descriptor, commandId, commandId, attemptId, input, executionContext, config);
    }

    public CommandRequest {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId must not be blank");
        }
        if (occurrenceId == null || occurrenceId.isBlank()) {
            throw new IllegalArgumentException("occurrenceId must not be blank");
        }
        if (attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("attemptId must not be blank");
        }
        if (executionContext == null) {
            throw new IllegalArgumentException("executionContext must not be null");
        }
        config = config == null ? Map.of() : Map.copyOf(config);
    }

    /** Stable provider idempotency key for this intentional effect occurrence. */
    public String providerIdempotencyKey() {
        return occurrenceId;
    }

    static String newAttemptId() {
        return "attempt-" + java.util.UUID.randomUUID();
    }
}
