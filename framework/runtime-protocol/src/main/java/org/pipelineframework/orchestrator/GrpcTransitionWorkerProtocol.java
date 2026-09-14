package org.pipelineframework.orchestrator;

/** Stable protocol constants for the gRPC transition-worker boundary. */
public final class GrpcTransitionWorkerProtocol {

    public static final String PROTOCOL_VERSION = "1";
    public static final String PAYLOAD_ENCODING = TransitionPayloadEncoding.JSON;
    public static final String SIGNATURE_METHOD = "GRPC";
    public static final String SIGNATURE_PATH =
        "/org.pipelineframework.orchestrator.grpc.TransitionWorkerService/Execute";
    public static final String CAPABILITIES_SIGNATURE_PATH =
        "/org.pipelineframework.orchestrator.grpc.TransitionWorkerService/Capabilities";

    private GrpcTransitionWorkerProtocol() {
    }
}
