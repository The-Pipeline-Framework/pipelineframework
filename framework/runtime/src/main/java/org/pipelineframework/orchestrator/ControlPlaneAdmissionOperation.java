package org.pipelineframework.orchestrator;

/**
 * Control-plane operation subject to local admission policy.
 */
public enum ControlPlaneAdmissionOperation {
    SUBMIT_EXECUTION,
    GET_EXECUTION_STATUS,
    GET_EXECUTION_RESULT,
    QUERY_PENDING_AWAIT,
    COMPLETE_AWAIT,
    PROCESS_WORK_ITEM
}
