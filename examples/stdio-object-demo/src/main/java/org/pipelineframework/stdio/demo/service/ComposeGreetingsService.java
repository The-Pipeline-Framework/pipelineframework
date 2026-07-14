package org.pipelineframework.stdio.demo.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.stdio.demo.common.domain.GreetingResponse;
import org.pipelineframework.stdio.demo.common.domain.NormalizedGreetings;

@PipelineStep
@ApplicationScoped
public final class ComposeGreetingsService implements ReactiveService<NormalizedGreetings, GreetingResponse> {
    @Override
    public Uni<GreetingResponse> process(NormalizedGreetings input) {
        return Uni.createFrom().item(new GreetingResponse(input.names().stream().map(name -> "Hello, " + name + "!").toList()));
    }
}
