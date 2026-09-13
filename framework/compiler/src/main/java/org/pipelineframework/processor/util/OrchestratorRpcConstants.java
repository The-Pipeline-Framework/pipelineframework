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
    public static final String RUN_ASYNC_METHOD = "RunAsync";
    public static final String GET_EXECUTION_STATUS_METHOD = "GetExecutionStatus";
    public static final String GET_EXECUTION_RESULT_METHOD = "GetExecutionResult";
    public static final String COMPLETE_AWAIT_METHOD = "CompleteAwait";
    public static final String LIST_PENDING_AWAIT_METHOD = "ListPendingAwait";
    public static final String INGEST_METHOD = "Ingest";
    public static final String SUBSCRIBE_METHOD = "Subscribe";
}
