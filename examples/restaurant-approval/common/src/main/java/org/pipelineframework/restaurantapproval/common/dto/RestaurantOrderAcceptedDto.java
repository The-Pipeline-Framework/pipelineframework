package org.pipelineframework.restaurantapproval.common.dto;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonTypeName("accepted")
public record RestaurantOrderAcceptedDto(
    UUID orderId,
    Instant decidedAt,
    String note
) implements RestaurantDecisionDto {

  public RestaurantOrderAcceptedDto {
    Objects.requireNonNull(orderId, "orderId must not be null");
    Objects.requireNonNull(decidedAt, "decidedAt must not be null");
    note = note == null ? "" : note.trim();
  }
}
