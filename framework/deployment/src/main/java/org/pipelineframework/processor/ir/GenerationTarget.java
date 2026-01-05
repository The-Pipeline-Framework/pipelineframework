package org.pipelineframework.processor.ir;

/**
 * Enum representing different artifact types to be generated.
 * Represents actual artifacts, not behavior.
 */
public enum GenerationTarget {
    /** gRPC client that delegates to gRPC stub */
    CLIENT_STEP,
    /** REST client step that delegates to a REST client */
    REST_CLIENT_STEP,
    /** gRPC server adapter */
    GRPC_SERVICE,
    /** JAX-RS resource */
    REST_RESOURCE,
}
