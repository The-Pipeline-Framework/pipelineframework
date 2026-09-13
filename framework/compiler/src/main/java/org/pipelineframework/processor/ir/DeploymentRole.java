package org.pipelineframework.processor.ir;

/**
 * Deployment roles for generated artifacts.
 */
public enum DeploymentRole {
    /** gRPC server adapters for application pipeline steps */
    PIPELINE_SERVER,
    /** Server implementations for external plugin services */
    PLUGIN_SERVER,
    /** gRPC client steps for orchestrator services */
    ORCHESTRATOR_CLIENT,
    /** Client stubs for calling external plugin services */
    PLUGIN_CLIENT,
    /** REST resources for HTTP-based access to pipeline steps */
    REST_SERVER
}
