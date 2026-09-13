package org.pipelineframework.command;

/**
 * Derives a deterministic command id from a command input.
 */
@FunctionalInterface
public interface CommandIdGenerator<I> {
    String commandId(CommandDescriptor descriptor, I input);
}
