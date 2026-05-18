package org.pipelineframework.tpfgo.common.domain;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public final class PaymentOutcomeJsonDeserializer extends JsonDeserializer<PaymentOutcome> {

    @Override
    public PaymentOutcome deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        String type = resolveType(node);
        if ("captured".equals(type)) {
            JsonNode captured = node.has("captured") ? node.get("captured") : node;
            return new PaymentCaptured(
                uuid(captured, "orderId"),
                uuid(captured, "paymentId"),
                instant(captured, "processedAt"),
                decimal(captured, "amount"),
                text(captured, "currency"));
        }
        if ("rejected".equals(type)) {
            JsonNode rejected = node.has("rejected") ? node.get("rejected") : node;
            return new PaymentRejected(
                uuid(rejected, "orderId"),
                instant(rejected, "processedAt"),
                decimal(rejected, "amount"),
                text(rejected, "currency"),
                text(rejected, "failureCode"),
                text(rejected, "failureReason"));
        }
        if ("requiresReview".equals(type)) {
            JsonNode review = node.has("requiresReview") ? node.get("requiresReview") : node;
            return new PaymentRequiresReview(
                uuid(review, "orderId"),
                instant(review, "processedAt"),
                decimal(review, "amount"),
                text(review, "currency"),
                text(review, "reviewReason"));
        }
        throw new IOException("Unsupported PaymentOutcome type: " + type);
    }

    private String resolveType(JsonNode node) {
        String type = node.path("type").asText(null);
        if (type != null && !type.isBlank()) {
            return type;
        }
        if (node.hasNonNull("captured")) {
            return "captured";
        }
        if (node.hasNonNull("rejected")) {
            return "rejected";
        }
        if (node.hasNonNull("requiresReview")) {
            return "requiresReview";
        }
        return null;
    }


    private UUID uuid(JsonNode node, String fieldName) {
        return UUID.fromString(text(node, fieldName));
    }

    private Instant instant(JsonNode node, String fieldName) {
        return Instant.parse(text(node, fieldName));
    }

    private BigDecimal decimal(JsonNode node, String fieldName) {
        return new BigDecimal(text(node, fieldName));
    }

    private String text(JsonNode node, String fieldName) {
        return node.path(fieldName).asText();
    }
}
