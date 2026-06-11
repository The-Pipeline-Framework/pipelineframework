package org.pipelineframework.orchestrator;

final class TransitionEnvelopeFixtures {

    private TransitionEnvelopeFixtures() {
    }

    static TransitionCommandEnvelope envelope(JsonTransitionPayloadCodec payloadCodec) {
        TransitionWorkerCommand command = new TransitionWorkerCommand(
            "tenant-1",
            "exec-1",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "exec-1:0:0",
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"));
        return TransitionCommandEnvelope.from(
            command,
            "local-pipeline",
            "local-bundle",
            "exec-1:0:0",
            payloadCodec.encode(command.inputPayload()));
    }
}
