package org.pipelineframework.tpfgo.runtime.journey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.BadRequestException;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.CheckpointPublicationRequest;

class CheckoutJourneyTraceResourceTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void recordsAndReturnsJourneyEvents() throws Exception {
        CheckoutJourneyTraceResource resource = resource();
        CheckoutJourneyTraceEvent recorded = resource.record(new CheckpointPublicationRequest(
            "tpfgo.restaurant.order-accepted.v1",
            JSON.readTree("""
            {"orderId":"order-4","requestId":"request-4","acceptedAt":"2026-06-13T11:00:00Z"}
            """)));

        CheckoutJourneyTraceResponse response = resource.journey("request-4", null);

        assertEquals("restaurant-acceptance", recorded.stageId());
        assertEquals(1, response.events().size());
        assertEquals("order-4", response.events().getFirst().orderId());
    }

    @Test
    void mapsBadRequestsToBadRequestException() {
        CheckoutJourneyTraceResource resource = resource();

        assertThrows(BadRequestException.class,
            () -> resource.record(null));
    }

    private CheckoutJourneyTraceResource resource() {
        CheckoutJourneyTraceResource resource = new CheckoutJourneyTraceResource();
        resource.traceStore = new CheckoutJourneyTraceStore();
        return resource;
    }
}
