package org.pipelineframework.processor.ir;

/**
 * Enum representing different artifact types to be generated.
 * Represents actual artifacts, not behavior.
 */
public enum GenerationTarget {
    /** gRPC client that delegates to gRPC stub */
    CLIENT_STEP,
    /** gRPC server adapter */
    GRPC_SERVICE,
    /** JAX-RS resource */
    REST_RESOURCE,
    /** Adapter that bridges gRPC messages to plugin implementations */
    PLUGIN_ADAPTER,
    /** Reactive service that delegates to plugin adapter */
    PLUGIN_REACTIVE_SERVICE
}