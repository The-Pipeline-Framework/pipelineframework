package org.pipelineframework.orchestrator.dto;

import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;

/**
 * Generic hosted-control-plane async execution submission.
 *
 * @param pipelineId logical pipeline id to execute
 * @param inputShape submitted input shape
 * @param inputPayload serialized pipeline input payload
 * @param idempotencyKey optional submission idempotency key
 * @param outputStreaming whether caller expects materialized multi-output retrieval
 */
public record HostedExecutionSubmitRequest(
    String pipelineId,
    ExecutionInputShape inputShape,
    SerializedTransitionPayload inputPayload,
    String idempotencyKey,
    boolean outputStreaming
) {
}
