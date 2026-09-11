package org.pipelineframework.examples.openapiproof;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.examples.agentproof.domain.StartJobRequest;

/** The application owns the stable business identity of an intentional provider job. */
@ApplicationScoped
public class JobCommandIdGenerator implements CommandIdGenerator<StartJobRequest> {
    @Override
    public String commandId(CommandDescriptor descriptor, StartJobRequest input) {
        return "job-" + input.jobId();
    }
}
