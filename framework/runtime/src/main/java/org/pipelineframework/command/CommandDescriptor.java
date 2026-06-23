package org.pipelineframework.command;

import java.util.Map;

/**
 * Runtime descriptor for one generated command step.
 */
public record CommandDescriptor(
    String stepId,
    String command,
    String inputType,
    String outputType,
    String commandIdGenerator,
    CommandDuplicatePolicy duplicatePolicy,
    Map<String, Object> config
) {
    public CommandDescriptor {
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("stepId must not be blank");
        }
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("command must not be blank");
        }
        if (inputType == null || inputType.isBlank()) {
            throw new IllegalArgumentException("inputType must not be blank");
        }
        if (outputType == null || outputType.isBlank()) {
            throw new IllegalArgumentException("outputType must not be blank");
        }
        if (commandIdGenerator == null || commandIdGenerator.isBlank()) {
            throw new IllegalArgumentException("commandIdGenerator must not be blank");
        }
        duplicatePolicy = duplicatePolicy == null ? CommandDuplicatePolicy.RETURN_RECORDED : duplicatePolicy;
        config = config == null ? Map.of() : Map.copyOf(config);
    }
}
