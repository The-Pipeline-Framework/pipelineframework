package org.pipelineframework.tpfgo.runtime.journey;

import java.util.List;

/**
 * Response body for the checkout demo journey trace endpoint.
 */
public record CheckoutJourneyTraceResponse(
    List<CheckoutJourneyTraceEvent> events
) {
}
