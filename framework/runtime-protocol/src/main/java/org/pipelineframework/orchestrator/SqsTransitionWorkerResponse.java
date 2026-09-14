package org.pipelineframework.orchestrator;

import java.util.Objects;

public record SqsTransitionWorkerResponse(
    String requestId,
    String protocolVersion,
    String resultEncoding,
    String resultEnvelope,
    String timestamp,
    String nonce,
    String signature
) {
    public SqsTransitionWorkerResponse {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(protocolVersion, "protocolVersion");
        Objects.requireNonNull(resultEncoding, "resultEncoding");
        Objects.requireNonNull(resultEnvelope, "resultEnvelope");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(nonce, "nonce");
        Objects.requireNonNull(signature, "signature");
    }
}
