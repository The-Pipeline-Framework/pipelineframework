package org.pipelineframework;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

interface TerminalPublicationBarrier {

    Uni<Void> publishBeforeSuccess(
        ExecutionRecord<Object, Object> record,
        TransitionResultEnvelope result);
}
