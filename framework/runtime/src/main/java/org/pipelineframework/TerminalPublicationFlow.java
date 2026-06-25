package org.pipelineframework;

import java.util.Objects;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

final class TerminalPublicationFlow implements TerminalPublicationBarrier {

    private final CheckpointPublicationService checkpointPublicationService;
    private final TerminalOutputPublisher terminalOutputPublisher;

    TerminalPublicationFlow(
        CheckpointPublicationService checkpointPublicationService,
        TerminalOutputPublisher terminalOutputPublisher) {
        this.checkpointPublicationService = checkpointPublicationService;
        this.terminalOutputPublisher = terminalOutputPublisher;
    }

    @Override
    public Uni<Void> publishBeforeSuccess(
        ExecutionRecord<Object, Object> record,
        TransitionResultEnvelope result) {
        Objects.requireNonNull(record, "record must not be null");
        Objects.requireNonNull(result, "result must not be null");

        TerminalPublicationPlan plan = TerminalPublicationPlan.from(record, result);
        Uni<Void> checkpointPublication = !plan.publishCheckpoint() || checkpointPublicationService == null
            ? Uni.createFrom().voidItem()
            : checkpointPublicationService.publishIfConfigured(plan.record(), plan.checkpointPayload());
        return checkpointPublication.chain(() -> publishTerminalOutputIfConfigured(plan));
    }

    private Uni<Void> publishTerminalOutputIfConfigured(TerminalPublicationPlan plan) {
        return !plan.publishTerminalOutput() || terminalOutputPublisher == null
            ? Uni.createFrom().voidItem()
            : terminalOutputPublisher.publishIfConfigured(plan.result());
    }
}
