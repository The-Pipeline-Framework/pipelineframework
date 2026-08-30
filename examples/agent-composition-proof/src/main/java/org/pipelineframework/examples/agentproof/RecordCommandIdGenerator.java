package org.pipelineframework.examples.agentproof;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.examples.agentproof.domain.RecordArguments;

@ApplicationScoped
public class RecordCommandIdGenerator implements CommandIdGenerator<RecordArguments> {
    @Override
    public String commandId(CommandDescriptor descriptor, RecordArguments input) {
        return descriptor.command() + ":" + input.action();
    }
}
