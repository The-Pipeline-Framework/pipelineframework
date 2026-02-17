package org.pipelineframework.processor.ir;

/**
 * Enum representing different artifact types to be generated.
 * Represents actual artifacts, not behavior.
 */
public enum GenerationTarget {
    /** gRPC client that delegates to gRPC stub */
    CLIENT_STEP,
    /** Local client step that delegates directly to the service */
    LOCAL_CLIENT_STEP,
    /** REST client step that delegates to a REST client */
    REST_CLIENT_STEP,
    /** gRPC server adapter */
    GRPC_SERVICE,
    /** Side-effect-only path for server roles under LOCAL transport */
    GRPC_SERVICE_SIDE_EFFECT_ONLY,
    /** JAX-RS resource */
    REST_RESOURCE,
    /** Orchestrator CLI entrypoint */
    ORCHESTRATOR_CLI,
    /** External adapter that delegates to operator services */
    EXTERNAL_ADAPTER,
}
