package org.pipelineframework.tpfgo.runtime.journey;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Demo-local checkpoint trace event for the TPFGo checkout UI.
 */
public record CheckoutJourneyTraceEvent(
    String publication,
    String stageId,
    long observedAtEpochMs,
    String requestId,
    String orderId,
    JsonNode payload
) {
}
