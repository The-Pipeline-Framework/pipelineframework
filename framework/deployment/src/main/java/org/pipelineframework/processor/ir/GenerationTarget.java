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
    /** Generated service implementation that dispatches to a remote operator endpoint */
    REMOTE_OPERATOR_ADAPTER,
    /** Generated reactive bridge for blocking-authored internal services */
    BLOCKING_REACTIVE_BRIDGE,
    /** Generated await client step that suspends queue-async execution */
    AWAIT_CLIENT_STEP,
    /** Generated command client step that executes managed external effects */
    COMMAND_CLIENT_STEP,
    /** Generated query client step that invokes a captured query connector */
    QUERY_CLIENT_STEP,
}
