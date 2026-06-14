package org.pipelineframework.tpfgo.runtime.journey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.CheckpointPublicationRequest;

class CheckoutJourneyTraceStoreTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final CheckoutJourneyTraceStore store = new CheckoutJourneyTraceStore();

    @Test
    void recordsCheckpointEventsAndExtractsCorrelationFields() {
        CheckoutJourneyTraceEvent event = store.record(request(
            "tpfgo.checkout.order-pending.v1",
            """
            {
              "orderId": "order-1",
              "requestId": "request-1",
              "totalAmount": "29.75"
            }
            """));

        assertEquals("checkout", event.stageId());
        assertEquals("request-1", event.requestId());
        assertEquals("order-1", event.orderId());
        assertEquals("29.75", event.payload().get("totalAmount").asText());
    }

    @Test
    void findsEventsByRequestIdOrOrderIdInObservationOrder() {
        store.record(request(
            "tpfgo.checkout.order-pending.v1",
            """
            {"orderId":"order-2","requestId":"request-2"}
            """));
        store.record(request(
            "tpfgo.kitchen.order-ready.v1",
            """
            {"orderId":"order-2","readyAt":"2026-06-13T10:00:00Z"}
            """));
        store.record(request(
            "tpfgo.checkout.order-pending.v1",
            """
            {"orderId":"other","requestId":"other-request"}
            """));

        List<CheckoutJourneyTraceEvent> byRequest = store.find("request-2", "");
        assertEquals(1, byRequest.size());
        assertEquals("checkout", byRequest.getFirst().stageId());

        List<CheckoutJourneyTraceEvent> byOrder = store.find("", "order-2");
        assertEquals(2, byOrder.size());
        assertEquals("checkout", byOrder.get(0).stageId());
        assertEquals("kitchen-preparation", byOrder.get(1).stageId());
    }

    @Test
    void extractsOrderIdFromNestedUnionPayloads() {
        CheckoutJourneyTraceEvent event = store.record(request(
            "tpfgo.payment.capture-result.v1",
            """
            {
              "captured": {
                "order_id": "order-3",
                "paymentId": "payment-1"
              }
            }
            """));

        assertEquals("payment", event.stageId());
        assertEquals("order-3", event.orderId());
    }

    @Test
    void keepsCheckpointPublicationRetriesIdempotent() {
        CheckoutJourneyTraceEvent first = store.record(request(
            "tpfgo.checkout.order-pending.v1",
            """
            {"orderId":"order-retry","requestId":"request-retry"}
            """));
        CheckoutJourneyTraceEvent second = store.record(request(
            "tpfgo.checkout.order-pending.v1",
            """
            {"orderId":"order-retry","requestId":"request-retry"}
            """));

        assertEquals(first.observedAtEpochMs(), second.observedAtEpochMs());
        assertEquals(1, store.find("request-retry", "order-retry").size());
    }

    @Test
    void rejectsNullPayloads() {
        assertThrows(IllegalArgumentException.class,
            () -> store.record(new CheckpointPublicationRequest(
                "tpfgo.checkout.order-pending.v1",
                JSON.nullNode())));
    }

    private CheckpointPublicationRequest request(String publication, String payloadJson) {
        try {
            JsonNode payload = JSON.readTree(payloadJson);
            return new CheckpointPublicationRequest(publication, payload);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
