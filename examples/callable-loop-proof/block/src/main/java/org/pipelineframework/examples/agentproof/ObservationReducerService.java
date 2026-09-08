package org.pipelineframework.examples.agentproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.examples.agentproof.domain.AgentState;
import org.pipelineframework.examples.agentproof.domain.OperationObservation;
import org.pipelineframework.service.ReactiveService;

/** Block-owned state evolution; dispatch itself never chooses another turn. */
@ApplicationScoped
public class ObservationReducerService implements ReactiveService<OperationObservation, AgentState> {
    @Override
    public Uni<AgentState> process(OperationObservation observation) {
        try {
            if (observation instanceof OperationObservation.Empty empty) {
                TrustedContext context = context(empty.value().contextJson());
                return Uni.createFrom().item(new AgentState(
                    context.state(), context.nextEffectKey(), empty.value().code(), "action"));
            }
            OperationObservation.Result result = (OperationObservation.Result) observation;
            TrustedContext context = context(result.value().contextJson());
            return Uni.createFrom().item(new AgentState(
                context.state(), context.nextEffectKey(), result.value().resultJson(), "complete"));
        } catch (Exception exception) {
            return Uni.createFrom().failure(exception);
        }
    }

    private TrustedContext context(String contextJson) throws Exception {
        return PipelineJson.mapper().readValue(contextJson, TrustedContext.class);
    }

    private record TrustedContext(String state, String evidence, String phase, String nextEffectKey) {
    }
}
