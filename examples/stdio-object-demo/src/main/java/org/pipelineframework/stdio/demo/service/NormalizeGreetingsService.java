package org.pipelineframework.stdio.demo.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.stdio.demo.common.domain.GreetingRequests;
import org.pipelineframework.stdio.demo.common.domain.NormalizedGreetings;

@PipelineStep
@ApplicationScoped
public final class NormalizeGreetingsService implements ReactiveService<GreetingRequests, NormalizedGreetings> {
    @Override
    public Uni<NormalizedGreetings> process(GreetingRequests input) {
        return Uni.createFrom().item(new NormalizedGreetings(input.names().stream().map(String::trim).toList()));
    }
}
