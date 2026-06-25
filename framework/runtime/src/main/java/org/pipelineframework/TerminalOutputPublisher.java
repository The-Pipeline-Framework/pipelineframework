package org.pipelineframework;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

interface TerminalOutputPublisher {

    Uni<Void> publishIfConfigured(TransitionResultEnvelope result);
}
