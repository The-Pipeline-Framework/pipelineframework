package org.pipelineframework.examples.agentproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.examples.agentproof.domain.ApplicationResult;
import org.pipelineframework.service.ReactiveService;
@ApplicationScoped
public class FinishService implements ReactiveService<ApplicationResult, ApplicationResult> {
    @Override
    public Uni<ApplicationResult> process(ApplicationResult input) {
        return Uni.createFrom().item(input);
    }
}
