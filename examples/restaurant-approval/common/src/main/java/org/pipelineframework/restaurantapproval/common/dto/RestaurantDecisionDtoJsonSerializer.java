package org.pipelineframework.restaurantapproval.common.dto;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public final class RestaurantDecisionDtoJsonSerializer extends JsonSerializer<RestaurantDecisionDto> {

  @Override
  public void serialize(RestaurantDecisionDto value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    if (value instanceof RestaurantOrderAcceptedDto accepted) {
      gen.writeStringField("type", "accepted");
      gen.writeObjectFieldStart("accepted");
      gen.writeStringField("orderId", accepted.orderId().toString());
      gen.writeStringField("decidedAt", accepted.decidedAt().toString());
      gen.writeStringField("note", accepted.note());
      gen.writeEndObject();
    } else if (value instanceof RestaurantOrderDeclinedDto declined) {
      gen.writeStringField("type", "declined");
      gen.writeObjectFieldStart("declined");
      gen.writeStringField("orderId", declined.orderId().toString());
      gen.writeStringField("decidedAt", declined.decidedAt().toString());
      gen.writeStringField("note", declined.note());
      gen.writeStringField("declineReason", declined.declineReason());
      gen.writeEndObject();
    } else {
      throw new IOException("Unsupported RestaurantDecisionDto type: " + value.getClass().getName());
    }
    gen.writeEndObject();
  }
}
