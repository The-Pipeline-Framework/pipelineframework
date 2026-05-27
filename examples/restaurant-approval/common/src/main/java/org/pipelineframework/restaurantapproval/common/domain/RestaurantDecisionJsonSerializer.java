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
    value.accept(new RestaurantDecisionVisitor<>() {
      @Override
      public Void accepted(RestaurantOrderAccepted accepted) {
        try {
          gen.writeStringField("type", "accepted");
          gen.writeObjectFieldStart("accepted");
          gen.writeStringField("orderId", accepted.orderId().toString());
          gen.writeStringField("decidedAt", accepted.decidedAt().toString());
          gen.writeStringField("note", accepted.note());
          gen.writeEndObject();
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        return null;
      }

      @Override
      public Void declined(RestaurantOrderDeclined declined) {
        try {
          gen.writeStringField("type", "declined");
          gen.writeObjectFieldStart("declined");
          gen.writeStringField("orderId", declined.orderId().toString());
          gen.writeStringField("decidedAt", declined.decidedAt().toString());
          gen.writeStringField("note", declined.note());
          gen.writeStringField("declineReason", declined.declineReason());
          gen.writeEndObject();
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        return null;
      }
    });
    gen.writeEndObject();
  }
}
