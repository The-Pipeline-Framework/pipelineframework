package org.pipelineframework.orchestrator;

final class GrpcTransitionWorkerProtocol {

    static final String PROTOCOL_VERSION = "1";
    static final String PAYLOAD_ENCODING = TransitionPayloadEncoding.JSON;
    static final String SIGNATURE_METHOD = "GRPC";
    static final String SIGNATURE_PATH = "/org.pipelineframework.orchestrator.grpc.TransitionWorkerService/Execute";
    static final String CAPABILITIES_SIGNATURE_PATH =
        "/org.pipelineframework.orchestrator.grpc.TransitionWorkerService/Capabilities";

    private GrpcTransitionWorkerProtocol() {
    }
}
