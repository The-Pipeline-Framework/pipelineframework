package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public final class RestaurantDecisionJsonSerializer extends JsonSerializer<RestaurantDecision> {

  @Override
  public void serialize(RestaurantDecision value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    if (value instanceof RestaurantOrderAccepted accepted) {
      gen.writeStringField("type", "accepted");
      gen.writeObjectFieldStart("accepted");
      gen.writeStringField("orderId", accepted.orderId().toString());
      gen.writeStringField("decidedAt", accepted.decidedAt().toString());
      gen.writeStringField("note", accepted.note());
      gen.writeEndObject();
    } else if (value instanceof RestaurantOrderDeclined declined) {
      gen.writeStringField("type", "declined");
      gen.writeObjectFieldStart("declined");
      gen.writeStringField("orderId", declined.orderId().toString());
      gen.writeStringField("decidedAt", declined.decidedAt().toString());
      gen.writeStringField("note", declined.note());
      gen.writeStringField("declineReason", declined.declineReason());
      gen.writeEndObject();
    }
    gen.writeEndObject();
  }
}
