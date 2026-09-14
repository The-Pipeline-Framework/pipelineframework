package org.pipelineframework.orchestrator;

import java.util.Objects;

public record SqsTransitionWorkerRequest(
    String requestId,
    String protocolVersion,
    String commandEncoding,
    String commandEnvelope,
    String timestamp,
    String nonce,
    String signature
) {
    public SqsTransitionWorkerRequest {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(protocolVersion, "protocolVersion");
        Objects.requireNonNull(commandEncoding, "commandEncoding");
        Objects.requireNonNull(commandEnvelope, "commandEnvelope");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(nonce, "nonce");
        Objects.requireNonNull(signature, "signature");
    }
}
