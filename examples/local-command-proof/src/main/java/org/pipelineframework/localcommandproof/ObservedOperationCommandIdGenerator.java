package org.pipelineframework.localcommandproof;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;

@ApplicationScoped
public class ObservedOperationCommandIdGenerator implements CommandIdGenerator<ObservedOperationCommand> {
    @Override
    public String commandId(CommandDescriptor descriptor, ObservedOperationCommand input) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        return descriptor.command() + ":" + input.operationId();
    }
}
