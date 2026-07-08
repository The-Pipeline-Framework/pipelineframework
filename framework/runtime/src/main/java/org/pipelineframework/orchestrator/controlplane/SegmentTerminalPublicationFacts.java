package org.pipelineframework.orchestrator.controlplane;

record SegmentTerminalPublicationFacts(
    String publicationId,
    String idempotencyKey,
    ControlPlaneFact.TerminalPublicationPrepared prepared,
    ControlPlaneFact.TerminalPublicationCompleted completed
) {
}
