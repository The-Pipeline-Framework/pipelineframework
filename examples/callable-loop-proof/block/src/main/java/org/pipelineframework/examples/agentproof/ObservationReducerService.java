package org.pipelineframework.examples.agentproof;

import java.util.Objects;

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
            String nextPhase = "lookup".equals(context.phase()) ? "action" : "complete";
            return Uni.createFrom().item(new AgentState(
                context.state(), context.nextEffectKey(), result.value().resultJson(), nextPhase));
        } catch (Exception exception) {
            return Uni.createFrom().failure(exception);
        }
    }

    private TrustedContext context(String contextJson) {
        try {
            return PipelineJson.mapper().readValue(contextJson, TrustedContext.class);
        } catch (Exception failure) {
            throw new IllegalArgumentException("operation observation contains invalid trusted context", failure);
        }
    }

    private record TrustedContext(String state, String evidence, String phase, String nextEffectKey) {
        private TrustedContext {
            state = requireNonBlank(state, "state");
            evidence = Objects.requireNonNull(evidence, "trusted context evidence must not be null");
            phase = requireNonBlank(phase, "phase");
            nextEffectKey = requireNonBlank(nextEffectKey, "nextEffectKey");
        }

        private static String requireNonBlank(String value, String field) {
            String checked = Objects.requireNonNull(value, "trusted context " + field + " must not be null").trim();
            if (checked.isEmpty()) {
                throw new IllegalArgumentException("trusted context " + field + " must not be blank");
            }
            return checked;
        }
    }
}
