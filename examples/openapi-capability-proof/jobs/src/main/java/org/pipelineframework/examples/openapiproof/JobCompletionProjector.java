package org.pipelineframework.examples.openapiproof;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.awaitable.AwaitCompletionMetadata;
import org.pipelineframework.awaitable.AwaitCompletionProjector;
import org.pipelineframework.examples.agentproof.domain.JobCallback;
import org.pipelineframework.examples.agentproof.domain.JobResult;
import org.pipelineframework.examples.agentproof.domain.StartJobRequest;

/** Pure projection from the original canonical request and authenticated provider completion. */
@ApplicationScoped
public class JobCompletionProjector implements AwaitCompletionProjector<StartJobRequest, JobCallback, JobResult> {
    @Override
    public JobResult project(StartJobRequest input, JobCallback completion, AwaitCompletionMetadata metadata) {
        if (!input.jobId().equals(completion.jobId())) {
            throw new IllegalArgumentException("Completion belongs to a different job");
        }
        return new JobResult(input.jobId(), input.value(), completion.status());
    }
}
