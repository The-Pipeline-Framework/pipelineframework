package org.pipelineframework.examples.agentproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.examples.agentproof.connector.ProofInvocationRecorder;
import org.pipelineframework.examples.agentproof.domain.AgentState;
import org.pipelineframework.examples.agentproof.domain.OperationObservation;
import org.pipelineframework.service.ReactiveService;
/** Application-owned state evolution; dispatch itself never chooses another turn. */
@ApplicationScoped
public class ObservationReducerService implements ReactiveService<OperationObservation, AgentState> {
    @Inject
    ProofInvocationRecorder recorder;

    @Override
    public Uni<AgentState> process(OperationObservation observation) {
        if (observation instanceof OperationObservation.Empty empty) {
            recorder.recordTransition("empty:" + empty.value().outcome() + ":" + empty.value().code());
            return Uni.createFrom().item(new AgentState(empty.value().code(), "action"));
        }
        OperationObservation.Result result = (OperationObservation.Result) observation;
        recorder.recordTransition("result:" + result.value().outcome() + ":" + result.value().code());
        return Uni.createFrom().item(new AgentState(result.value().resultJson(), "complete"));
    }
}
