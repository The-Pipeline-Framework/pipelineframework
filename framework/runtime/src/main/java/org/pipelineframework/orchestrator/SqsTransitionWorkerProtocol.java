package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;

final class SqsTransitionWorkerProtocol {

    static final String PROTOCOL_VERSION = "1";
    static final String PAYLOAD_ENCODING = "application/tpf-transition-envelope+json";
    static final String SIGNATURE_METHOD = "SQS";
    static final String REQUEST_SIGNATURE_PATH = "/pipeline/worker/transitions/sqs/request";
    static final String RESPONSE_SIGNATURE_PATH = "/pipeline/worker/transitions/sqs/response";

    private SqsTransitionWorkerProtocol() {
    }

    static byte[] signedBytes(String requestId, String envelopeJson) {
        return (requestId + "\n" + envelopeJson).getBytes(StandardCharsets.UTF_8);
    }
}
