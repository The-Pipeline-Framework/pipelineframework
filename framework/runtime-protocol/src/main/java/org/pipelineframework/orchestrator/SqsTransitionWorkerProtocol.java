package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;

/** Stable protocol constants and canonical payload framing for SQS transition workers. */
public final class SqsTransitionWorkerProtocol {

    public static final String PROTOCOL_VERSION = "1";
    public static final String PAYLOAD_ENCODING = "application/tpf-transition-envelope+json";
    public static final String SIGNATURE_METHOD = "SQS";
    public static final String REQUEST_SIGNATURE_PATH = "/pipeline/worker/transitions/sqs/request";
    public static final String RESPONSE_SIGNATURE_PATH = "/pipeline/worker/transitions/sqs/response";

    private SqsTransitionWorkerProtocol() {
    }

    public static byte[] signedBytes(String requestId, String envelopeJson) {
        return (requestId + "\n" + envelopeJson).getBytes(StandardCharsets.UTF_8);
    }
}
