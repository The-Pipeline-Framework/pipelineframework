package org.pipelineframework.processor.util;

/**
 * Shared orchestrator RPC method names.
 */
public final class OrchestratorRpcConstants {

    /**
     * Prevents instantiation of this utility class which only exposes static RPC method name constants.
     */
    private OrchestratorRpcConstants() {
    }

    public static final String RUN_METHOD = "Run";
    public static final String INGEST_METHOD = "Ingest";
    public static final String SUBSCRIBE_METHOD = "Subscribe";
}