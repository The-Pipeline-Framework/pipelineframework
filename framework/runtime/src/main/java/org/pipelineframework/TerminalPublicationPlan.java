package org.pipelineframework;

import java.util.List;

import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

record TerminalPublicationPlan(
    ExecutionRecord<Object, Object> record,
    TransitionResultEnvelope result,
    List<?> outputItems,
    boolean publishCheckpoint,
    boolean publishTerminalOutput) {

    TerminalPublicationPlan {
        outputItems = List.copyOf(outputItems);
    }

    static TerminalPublicationPlan from(
        ExecutionRecord<Object, Object> record,
        TransitionResultEnvelope result) {
        List<?> outputItems = result.coordinatorOutputItems();
        return new TerminalPublicationPlan(
            record,
            result,
            outputItems,
            !outputItems.isEmpty(),
            !result.terminalOutputPublished());
    }

    Object checkpointPayload() {
        return outputItems.getFirst();
    }
}
