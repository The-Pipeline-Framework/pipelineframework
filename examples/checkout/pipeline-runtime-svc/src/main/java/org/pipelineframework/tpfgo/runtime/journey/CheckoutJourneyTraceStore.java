package org.pipelineframework.tpfgo.runtime.journey;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.checkpoint.CheckpointPublicationRequest;

/**
 * In-memory trace store for the TPFGo checkout demo.
 */
@ApplicationScoped
public class CheckoutJourneyTraceStore {

    static final int MAX_EVENTS = 512;

    private static final Map<String, String> STAGE_BY_PUBLICATION = Map.of(
        "tpfgo.checkout.order-pending.v1", "checkout",
        "tpfgo.consumer.order-approved.v1", "consumer-approval",
        "tpfgo.restaurant.order-accepted.v1", "restaurant-acceptance",
        "tpfgo.kitchen.order-ready.v1", "kitchen-preparation",
        "tpfgo.dispatch.delivery-assigned.v1", "dispatch",
        "tpfgo.delivery.order-delivered.v1", "delivery",
        "tpfgo.payment.capture-result.v1", "payment",
        "tpfgo.compensation.terminal-state.v1", "terminal");

    private final CopyOnWriteArrayList<CheckoutJourneyTraceEvent> events = new CopyOnWriteArrayList<>();

    public synchronized CheckoutJourneyTraceEvent record(CheckpointPublicationRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("checkpoint trace request must not be null");
        }
        JsonNode payload = request.payload();
        if (payload == null || payload.isNull()) {
            throw new IllegalArgumentException("checkpoint trace request must include a payload");
        }
        String publication = normalize(request.publication());
        if (publication == null) {
            throw new IllegalArgumentException("checkpoint trace request must include a publication");
        }
        String requestId = normalize(firstText(payload, "requestId", "request_id"));
        String orderId = normalize(firstText(payload, "orderId", "order_id"));
        if (requestId == null && orderId == null) {
            throw new IllegalArgumentException("checkpoint trace request must include requestId or orderId");
        }
        CheckoutJourneyTraceEvent existing = existingEvent(publication, requestId, orderId);
        if (existing != null) {
            return existing;
        }

        CheckoutJourneyTraceEvent event = new CheckoutJourneyTraceEvent(
            publication,
            STAGE_BY_PUBLICATION.getOrDefault(publication, ""),
            System.currentTimeMillis(),
            requestId,
            orderId,
            payload);
        events.add(event);
        trim();
        return event;
    }

    public List<CheckoutJourneyTraceEvent> find(String requestId, String orderId) {
        String normalizedRequestId = normalize(requestId);
        String normalizedOrderId = normalize(orderId);
        if (normalizedRequestId == null && normalizedOrderId == null) {
            return List.of();
        }
        return events.stream()
            .filter(event -> matches(event, normalizedRequestId, normalizedOrderId))
            .sorted(Comparator.comparingLong(CheckoutJourneyTraceEvent::observedAtEpochMs))
            .toList();
    }

    void clear() {
        events.clear();
    }

    private boolean matches(CheckoutJourneyTraceEvent event, String requestId, String orderId) {
        return (requestId != null && requestId.equals(event.requestId()))
            || (orderId != null && orderId.equals(event.orderId()));
    }

    private CheckoutJourneyTraceEvent existingEvent(String publication, String requestId, String orderId) {
        for (CheckoutJourneyTraceEvent event : events) {
            if (sameTracePoint(event, publication, requestId, orderId)) {
                return event;
            }
        }
        return null;
    }

    private boolean sameTracePoint(
        CheckoutJourneyTraceEvent event,
        String publication,
        String requestId,
        String orderId
    ) {
        if (!event.publication().equals(publication)) {
            return false;
        }
        String normalizedRequestId = normalize(requestId);
        String normalizedOrderId = normalize(orderId);
        return (normalizedRequestId != null && normalizedRequestId.equals(event.requestId()))
            || (normalizedOrderId != null && normalizedOrderId.equals(event.orderId()));
    }

    private void trim() {
        int extra = events.size() - MAX_EVENTS;
        if (extra <= 0) {
            return;
        }
        for (int i = 0; i < extra; i++) {
            events.remove(0);
        }
    }

    private static String firstText(JsonNode node, String... names) {
        if (node == null || node.isNull()) {
            return "";
        }
        for (String name : names) {
            JsonNode found = findField(node, normalizeName(name));
            if (found != null && !found.isNull()) {
                if (found.isTextual()) {
                    return found.asText();
                }
                if (found.isNumber() || found.isBoolean()) {
                    return found.asText();
                }
            }
        }
        return "";
    }

    private static JsonNode findField(JsonNode node, String normalizedName) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isObject()) {
            var fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                if (normalizeName(entry.getKey()).equals(normalizedName)) {
                    return entry.getValue();
                }
                JsonNode nested = findField(entry.getValue(), normalizedName);
                if (nested != null) {
                    return nested;
                }
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                JsonNode nested = findField(child, normalizedName);
                if (nested != null) {
                    return nested;
                }
            }
        }
        return null;
    }

    private static String normalizeName(String value) {
        return value == null ? "" : value.replace("_", "").toLowerCase(Locale.ROOT);
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
