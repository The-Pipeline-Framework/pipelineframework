package org.pipelineframework.restaurantapproval.common.dto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

public final class RestaurantDecisionDtoJsonDeserializer extends JsonDeserializer<RestaurantDecisionDto> {

  @Override
  public RestaurantDecisionDto deserialize(JsonParser parser, DeserializationContext context)
      throws IOException {
    JsonNode node = parser.getCodec().readTree(parser);
    String type = resolveType(node);
    if ("accepted".equals(type)) {
      JsonNode payload = node.has("accepted") ? node.get("accepted") : node;
      return new RestaurantOrderAcceptedDto(
          uuid(payload, "orderId"),
          instant(payload, "decidedAt"),
          text(payload, "note"));
    }
    if ("declined".equals(type)) {
      JsonNode payload = node.has("declined") ? node.get("declined") : node;
      return new RestaurantOrderDeclinedDto(
          uuid(payload, "orderId"),
          instant(payload, "decidedAt"),
          text(payload, "note"),
          text(payload, "declineReason"));
    }
    throw new IOException("Unsupported RestaurantDecisionDto type: " + type);
  }

  private String resolveType(JsonNode node) {
    String type = node.path("type").asText(null);
    if (type != null && !type.isBlank()) {
      return type;
    }
    if (node.hasNonNull("accepted")) {
      return "accepted";
    }
    if (node.hasNonNull("declined")) {
      return "declined";
    }
    if (node.hasNonNull("declineReason")) {
      return "declined";
    }
    return null;
  }

  private static UUID uuid(JsonNode node, String fieldName) {
    String value = text(node, fieldName);
    return value == null || value.isBlank() ? null : UUID.fromString(value);
  }

  private static Instant instant(JsonNode node, String fieldName) {
    String value = text(node, fieldName);
    return value == null || value.isBlank() ? null : Instant.parse(value);
  }

  private static String text(JsonNode node, String fieldName) {
    JsonNode value = node.get(fieldName);
    return value == null || value.isNull() ? null : value.asText();
  }
}
